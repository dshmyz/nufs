package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type fakeS3Object struct {
	data         []byte
	lastModified time.Time
}

type fakeS3API struct {
	mu                      sync.Mutex
	objects                 map[string]fakeS3Object
	ops                     []string
	bodies                  []*trackedS3Body
	deleteBatches           [][]string
	ifNoneMatch             map[string]string
	copySources             []string
	pageSize                int
	failOp                  int
	deleteErrors            map[string]string
	repeatToken             bool
	nilNilGetKey            string
	errorBodyKey            string
	afterList               map[string]func()
	storeThenErrorKey       string
	putErrorThenGetErrorKey string
	getErrors               map[string]error
}

type trackedS3Body struct {
	*bytes.Reader
	closed bool
}

func (b *trackedS3Body) Close() error {
	b.closed = true
	return nil
}

func newFakeS3API() *fakeS3API {
	return &fakeS3API{
		objects:      make(map[string]fakeS3Object),
		deleteErrors: make(map[string]string),
		ifNoneMatch:  make(map[string]string),
		afterList:    make(map[string]func()),
		getErrors:    make(map[string]error),
	}
}

func (f *fakeS3API) record(op string) error {
	f.ops = append(f.ops, op)
	if f.failOp > 0 && len(f.ops) == f.failOp {
		return errors.New("injected s3 failure")
	}
	return nil
}

func (f *fakeS3API) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := aws.ToString(input.Key)
	if err := f.record("put " + key); err != nil {
		return nil, err
	}
	if input.IfNoneMatch != nil {
		f.ifNoneMatch[key] = aws.ToString(input.IfNoneMatch)
		if _, exists := f.objects[key]; exists {
			return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "object already exists"}
		}
	}
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	if key == f.putErrorThenGetErrorKey {
		f.getErrors[key] = errors.New("injected marker reconciliation GET failure")
		return nil, errors.New("injected marker Put failure")
	}
	f.objects[key] = fakeS3Object{data: data, lastModified: time.Now().UTC()}
	if key == f.storeThenErrorKey {
		return nil, errors.New("injected response loss after marker storage")
	}
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3API) CopyObject(_ context.Context, input *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := aws.ToString(input.Key)
	if err := f.record("copy " + key); err != nil {
		return nil, err
	}
	f.copySources = append(f.copySources, aws.ToString(input.CopySource))
	source, err := url.PathUnescape(aws.ToString(input.CopySource))
	if err != nil {
		return nil, err
	}
	source = strings.TrimPrefix(source, aws.ToString(input.Bucket)+"/")
	object, ok := f.objects[source]
	if !ok {
		return nil, fmt.Errorf("source object %q not found", source)
	}
	f.objects[key] = fakeS3Object{data: append([]byte(nil), object.data...), lastModified: time.Now().UTC()}
	return &s3.CopyObjectOutput{}, nil
}

func (f *fakeS3API) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := aws.ToString(input.Key)
	if err := f.record("get " + key); err != nil {
		return nil, err
	}
	if key == f.nilNilGetKey {
		return nil, nil
	}
	if key == f.errorBodyKey {
		body := &trackedS3Body{Reader: bytes.NewReader([]byte("error body"))}
		f.bodies = append(f.bodies, body)
		return &s3.GetObjectOutput{Body: body}, errors.New("injected get error with body")
	}
	if err := f.getErrors[key]; err != nil {
		return nil, err
	}
	object, ok := f.objects[key]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey", Message: fmt.Sprintf("object %q not found", key)}
	}
	body := &trackedS3Body{Reader: bytes.NewReader(object.data)}
	f.bodies = append(f.bodies, body)
	size := int64(len(object.data))
	return &s3.GetObjectOutput{Body: body, ContentLength: &size}, nil
}

func (f *fakeS3API) DeleteObjects(_ context.Context, input *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var keys []string
	for _, object := range input.Delete.Objects {
		keys = append(keys, aws.ToString(object.Key))
	}
	f.deleteBatches = append(f.deleteBatches, append([]string(nil), keys...))
	if err := f.record("delete " + strings.Join(keys, ",")); err != nil {
		return nil, err
	}
	output := &s3.DeleteObjectsOutput{}
	for _, key := range keys {
		if message, ok := f.deleteErrors[key]; ok {
			output.Errors = append(output.Errors, types.Error{
				Code:    aws.String("Injected"),
				Key:     aws.String(key),
				Message: aws.String(message),
			})
			continue
		}
		delete(f.objects, key)
	}
	return output, nil
}

func (f *fakeS3API) ListObjectsV2(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	var afterList func()
	defer func() {
		f.mu.Unlock()
		if afterList != nil {
			afterList()
		}
	}()
	prefix := aws.ToString(input.Prefix)
	if err := f.record("list " + prefix); err != nil {
		return nil, err
	}
	var keys []string
	for key := range f.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	start := 0
	if input.ContinuationToken != nil && !f.repeatToken {
		var err error
		start, err = strconv.Atoi(aws.ToString(input.ContinuationToken))
		if err != nil {
			return nil, err
		}
	}
	end := len(keys)
	if f.pageSize > 0 && end-start > f.pageSize {
		end = start + f.pageSize
	}
	output := &s3.ListObjectsV2Output{}
	for _, key := range keys[start:end] {
		object := f.objects[key]
		size := int64(len(object.data))
		output.Contents = append(output.Contents, types.Object{
			Key:          aws.String(key),
			LastModified: aws.Time(object.lastModified),
			Size:         &size,
		})
	}
	if end < len(keys) {
		output.IsTruncated = aws.Bool(true)
		output.NextContinuationToken = aws.String(strconv.Itoa(end))
	} else {
		output.IsTruncated = aws.Bool(false)
	}
	if f.repeatToken {
		output.IsTruncated = aws.Bool(true)
		if input.ContinuationToken == nil {
			output.NextContinuationToken = aws.String("repeat")
		} else {
			output.NextContinuationToken = input.ContinuationToken
		}
	}
	afterList = f.afterList[prefix]
	delete(f.afterList, prefix)
	return output, nil
}

func TestS3BackupRepositoryPublishesCommittedMarkerLast(t *testing.T) {
	client := newFakeS3API()
	repo, err := newS3BackupRepository(client, "bucket", "tenant/metadata")
	if err != nil {
		t.Fatal(err)
	}
	checkpointDir, manifest := createManifestFixture(t)
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}

	base := "tenant/metadata/"
	claimKey := base + "claims/" + manifest.BackupID + ".json"
	claim, err := decodeS3Claim(client.objects[claimKey].data)
	if err != nil {
		t.Fatal(err)
	}
	stageBase := base + "staging/" + manifest.BackupID + "/" + claim.AttemptID + "/"
	want := []string{
		"get " + base + "backups/" + manifest.BackupID + "/COMMITTED",
		"put " + claimKey,
	}
	for _, file := range manifest.Files {
		want = append(want, "put "+stageBase+"files/"+file.Path)
	}
	want = append(want, "put "+stageBase+"manifest.json", "get "+claimKey)
	for _, file := range manifest.Files {
		want = append(want, "copy "+base+"backups/"+manifest.BackupID+"/files/"+file.Path)
	}
	want = append(want,
		"copy "+base+"backups/"+manifest.BackupID+"/manifest.json",
		"get "+claimKey,
		"put "+base+"backups/"+manifest.BackupID+"/COMMITTED",
	)
	if strings.Join(client.ops, "\n") != strings.Join(want, "\n") {
		t.Fatalf("operation order:\n%s\nwant:\n%s", strings.Join(client.ops, "\n"), strings.Join(want, "\n"))
	}
	markerObject := client.objects[base+"backups/"+manifest.BackupID+"/COMMITTED"]
	marker, err := decodeS3CommitMarker(markerObject.data)
	if err != nil {
		t.Fatal(err)
	}
	if marker != commitMarkerFromClaim(claim) {
		t.Fatalf("COMMITTED = %+v, want claim publication %+v", marker, commitMarkerFromClaim(claim))
	}
	if got := client.ifNoneMatch[base+"backups/"+manifest.BackupID+"/COMMITTED"]; got != "*" {
		t.Fatalf("COMMITTED If-None-Match = %q, want *", got)
	}
	if got := client.ifNoneMatch[claimKey]; got != "*" {
		t.Fatalf("claim If-None-Match = %q, want *", got)
	}
	for _, source := range client.copySources {
		decoded, err := url.PathUnescape(source)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(decoded, "bucket/"+base+"staging/") {
			t.Fatalf("CopySource %q escaped bucket/prefix", decoded)
		}
	}
}

func TestS3BackupRepositoryFailuresNeverBecomeVisible(t *testing.T) {
	checkpointDir, manifest := createManifestFixture(t)
	fileCount := len(manifest.Files)
	failurePoints := []int{2, fileCount + 2, fileCount + 3, 2*fileCount + 3, 2*fileCount + 4}
	for _, failOp := range failurePoints {
		t.Run(strconv.Itoa(failOp), func(t *testing.T) {
			client := newFakeS3API()
			client.failOp = failOp
			repo, err := newS3BackupRepository(client, "bucket", "prefix")
			if err != nil {
				t.Fatal(err)
			}
			if err := repo.Publish(context.Background(), checkpointDir, manifest); err == nil {
				t.Fatal("Publish returned nil")
			}
			client.failOp = 0
			descriptors, err := repo.ListCommitted(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(descriptors) != 0 {
				t.Fatalf("failed publish listed as committed: %v", descriptors)
			}
			if _, err := repo.Fetch(context.Background(), manifest.BackupID, filepath.Join(realTempDir(t), "restore")); err == nil {
				t.Fatal("Fetch returned nil after failed publish")
			}
		})
	}
}

func TestS3BackupRepositoryListsAllPagesNewestFirst(t *testing.T) {
	client := newFakeS3API()
	client.pageSize = 1
	repo, err := newS3BackupRepository(client, "bucket", "prefix/")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	for _, item := range []struct {
		id string
		at time.Time
	}{
		{id: "older", at: base.Add(-time.Hour)},
		{id: "newer", at: base},
	} {
		checkpointDir, manifest := createManifestFixture(t)
		manifest.BackupID = item.id
		manifest.CreatedAt = item.at
		if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
			t.Fatal(err)
		}
	}
	client.ops = nil
	got, err := repo.ListCommitted(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "newer" || got[1].ID != "older" {
		t.Fatalf("ListCommitted = %v", got)
	}
	listCalls := 0
	for _, op := range client.ops {
		if strings.HasPrefix(op, "list ") {
			listCalls++
		}
	}
	if listCalls < 2 {
		t.Fatalf("ListCommitted made %d list calls, want pagination", listCalls)
	}
}

func TestS3BackupRepositoryFetchUsesDirectGetsAndClosesBodies(t *testing.T) {
	client := newFakeS3API()
	repo, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	checkpointDir, manifest := createManifestFixture(t)
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}
	client.ops = nil
	client.bodies = nil
	target := filepath.Join(t.TempDir(), "restore")
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, target); err != nil {
		t.Fatal(err)
	}
	for _, op := range client.ops {
		if strings.HasPrefix(op, "list ") {
			t.Fatalf("Fetch used ListObjectsV2: %v", client.ops)
		}
	}
	if len(client.bodies) != len(manifest.Files)+2 {
		t.Fatalf("GetObject body count = %d, want %d", len(client.bodies), len(manifest.Files)+2)
	}
	for i, body := range client.bodies {
		if !body.closed {
			t.Fatalf("GetObject body %d was not closed", i)
		}
	}
}

func TestS3BackupRepositoryFetchIsExclusiveAndCleansCorruption(t *testing.T) {
	client := newFakeS3API()
	repo, err := newS3BackupRepository(client, "bucket", "")
	if err != nil {
		t.Fatal(err)
	}
	checkpointDir, manifest := createManifestFixture(t)
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}

	target := realTempDir(t)
	existing := filepath.Join(target, filepath.FromSlash(manifest.Files[0].Path))
	if err := os.MkdirAll(filepath.Dir(existing), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(existing, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, target); err == nil {
		t.Fatal("Fetch overwrote an existing file")
	}
	data, err := os.ReadFile(existing)
	if err != nil || string(data) != "keep" {
		t.Fatalf("existing file = %q, %v", data, err)
	}

	fileKey := "backups/" + manifest.BackupID + "/files/" + manifest.Files[0].Path
	client.objects[fileKey] = fakeS3Object{data: []byte("corrupt"), lastModified: time.Now()}
	corruptTarget := filepath.Join(realTempDir(t), "corrupt")
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, corruptTarget); err == nil {
		t.Fatal("Fetch accepted corrupted artifact")
	}
	if _, err := os.Stat(corruptTarget); !os.IsNotExist(err) {
		t.Fatalf("corrupt target stat = %v, want cleaned", err)
	}
	for i, body := range client.bodies {
		if !body.closed {
			t.Fatalf("body %d remained open after failure", i)
		}
	}
}

func TestS3BackupRepositoryRejectsAncestorSymlinkAndReplacement(t *testing.T) {
	client := newFakeS3API()
	repo, err := newS3BackupRepository(client, "bucket", "")
	if err != nil {
		t.Fatal(err)
	}
	checkpointDir, manifest := createManifestFixture(t)
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}

	base := realTempDir(t)
	outside := realTempDir(t)
	existingOutside := filepath.Join(outside, "existing")
	if err := os.Mkdir(existingOutside, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Fetch(
		context.Background(),
		manifest.BackupID,
		filepath.Join(link, "existing", "restore"),
	); err == nil {
		t.Fatal("Fetch followed ancestor symlink")
	}
	if _, err := os.Stat(filepath.Join(existingOutside, "restore")); !os.IsNotExist(err) {
		t.Fatalf("ancestor symlink target was modified: %v", err)
	}

	target := filepath.Join(base, "replacement")
	repo.beforeOpenTarget = func(path string) error {
		if err := os.Remove(path); err != nil {
			return err
		}
		return os.Symlink(outside, path)
	}
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, target); err == nil {
		t.Fatal("Fetch accepted target replaced by symlink")
	}
	entries, err := os.ReadDir(existingOutside)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("replacement symlink target was modified: %v", entries)
	}

	ordinaryTarget := filepath.Join(base, "ordinary-replacement")
	sentinel := filepath.Join(ordinaryTarget, "caller-owned.txt")
	repo.beforeOpenTarget = func(path string) error {
		if err := os.Remove(path); err != nil {
			return err
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			return err
		}
		return os.WriteFile(sentinel, []byte("keep"), 0o600)
	}
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, ordinaryTarget); err == nil {
		t.Fatal("Fetch accepted target replaced by ordinary directory")
	}
	data, err := os.ReadFile(sentinel)
	if err != nil || string(data) != "keep" {
		t.Fatalf("caller-owned replacement content = %q, %v", data, err)
	}
}

func TestS3BackupRepositoryRejectsPrefixEscapes(t *testing.T) {
	for _, prefix := range []string{"/absolute", "../escape", "a/../escape", "a//b", `a\b`} {
		t.Run(prefix, func(t *testing.T) {
			if _, err := newS3BackupRepository(newFakeS3API(), "bucket", prefix); err == nil {
				t.Fatalf("accepted prefix %q", prefix)
			}
		})
	}
}

func TestS3BackupRepositoryDeleteBatchesAndInspectsErrors(t *testing.T) {
	client := newFakeS3API()
	repo, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1001; i++ {
		key := fmt.Sprintf("prefix/backups/large/files/%04d", i)
		client.objects[key] = fakeS3Object{}
	}
	client.objects["prefix/backups/large/COMMITTED"] = fakeS3Object{}
	client.objects["prefix/claims/large.json"] = fakeS3Object{}
	if err := repo.Delete(context.Background(), "large"); err != nil {
		t.Fatal(err)
	}
	if len(client.deleteBatches) != 5 {
		t.Fatalf("DeleteObjects batches = %d, want 2 data + claim + manifest + marker", len(client.deleteBatches))
	}
	for i, batch := range client.deleteBatches {
		if len(batch) > 1000 {
			t.Fatalf("batch %d size = %d, exceeds S3 limit", i, len(batch))
		}
	}
	if err := repo.Delete(context.Background(), "missing"); err != nil {
		t.Fatalf("missing Delete: %v", err)
	}

	client.objects["prefix/backups/error/COMMITTED"] = fakeS3Object{}
	client.deleteErrors["prefix/backups/error/COMMITTED"] = "denied"
	if err := repo.Delete(context.Background(), "error"); err == nil {
		t.Fatal("Delete ignored per-object error")
	}
}

func TestS3BackupRepositoryHonorsCanceledContext(t *testing.T) {
	client := newFakeS3API()
	repo, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	checkpointDir, manifest := createManifestFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := repo.Publish(ctx, checkpointDir, manifest); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish error = %v, want context canceled", err)
	}
	if len(client.ops) != 0 {
		t.Fatalf("canceled Publish made S3 calls: %v", client.ops)
	}
}

func TestS3BackupRepositoryDeleteStagingOlderThan(t *testing.T) {
	client := newFakeS3API()
	client.pageSize = 1
	repo, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	cutoff := time.Now().UTC().Add(-time.Hour)
	oldAttempt := strings.Repeat("a", 32)
	newAttempt := strings.Repeat("b", 32)
	for key, modified := range map[string]time.Time{
		"prefix/staging/old/" + oldAttempt + "/files/a":       cutoff.Add(-time.Hour),
		"prefix/staging/old/" + oldAttempt + "/manifest.json": cutoff.Add(-time.Hour),
		"prefix/staging/new/" + newAttempt + "/files/a":       cutoff.Add(time.Hour),
	} {
		client.objects[key] = fakeS3Object{lastModified: modified}
	}
	if err := repo.DeleteStagingOlderThan(context.Background(), cutoff); err != nil {
		t.Fatal(err)
	}
	if _, ok := client.objects["prefix/staging/old/"+oldAttempt+"/files/a"]; ok {
		t.Fatal("old staging file was not deleted")
	}
	if _, ok := client.objects["prefix/staging/old/"+oldAttempt+"/manifest.json"]; ok {
		t.Fatal("old staging manifest was not deleted")
	}
	if _, ok := client.objects["prefix/staging/new/"+newAttempt+"/files/a"]; !ok {
		t.Fatal("new staging file was deleted")
	}
}

func TestS3BackupRepositoryRejectsMalformedBackupIDs(t *testing.T) {
	repo, err := newS3BackupRepository(newFakeS3API(), "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"", ".", "..", "../escape", `a\b`, "C:escape"} {
		t.Run(id, func(t *testing.T) {
			if err := repo.Delete(context.Background(), id); err == nil {
				t.Fatalf("Delete accepted backup ID %q", id)
			}
			if _, err := repo.Fetch(context.Background(), id, filepath.Join(realTempDir(t), "restore")); err == nil {
				t.Fatalf("Fetch accepted backup ID %q", id)
			}
		})
	}
}

func TestS3BackupRepositoryDoesNotOverwriteDifferentCommittedBackup(t *testing.T) {
	client := newFakeS3API()
	repo, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	checkpointDir, manifest := createManifestFixture(t)
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}
	client.ops = nil
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatalf("idempotent Publish: %v", err)
	}
	for _, op := range client.ops {
		if strings.HasPrefix(op, "put ") || strings.HasPrefix(op, "copy ") {
			t.Fatalf("idempotent Publish wrote object: %v", client.ops)
		}
	}

	different := *manifest
	different.AppliedIndex++
	client.ops = nil
	if err := repo.Publish(context.Background(), checkpointDir, &different); err == nil {
		t.Fatal("Publish overwrote a committed backup with different contents")
	}
	for _, op := range client.ops {
		if strings.HasPrefix(op, "put ") || strings.HasPrefix(op, "copy ") {
			t.Fatalf("conflicting Publish wrote object: %v", client.ops)
		}
	}
}

func TestS3BackupRepositoryClaimPreventsMultiInstanceMixing(t *testing.T) {
	client := newFakeS3API()
	repoA, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	repoB, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	checkpointDir, manifestA := createManifestFixture(t)
	manifestB := *manifestA
	manifestB.AppliedIndex++
	claimed := make(chan s3BackupClaim, 1)
	release := make(chan struct{})
	repoA.afterClaim = func(claim s3BackupClaim) error {
		claimed <- claim
		<-release
		return nil
	}
	errA := make(chan error, 1)
	go func() { errA <- repoA.Publish(context.Background(), checkpointDir, manifestA) }()
	claim := <-claimed

	before := len(client.ops)
	if err := repoB.Publish(context.Background(), checkpointDir, &manifestB); err == nil {
		t.Fatal("different publisher acquired an active claim")
	}
	for _, op := range client.ops[before:] {
		if strings.HasPrefix(op, "copy ") || strings.Contains(op, "/staging/") && strings.HasPrefix(op, "put ") {
			t.Fatalf("claim loser mutated staging/final objects: %v", client.ops[before:])
		}
	}
	close(release)
	if err := <-errA; err != nil {
		t.Fatal(err)
	}
	fetched, err := repoB.Fetch(context.Background(), manifestA.BackupID, filepath.Join(realTempDir(t), "restore"))
	if err != nil {
		t.Fatal(err)
	}
	if fetched.AppliedIndex != manifestA.AppliedIndex {
		t.Fatalf("fetched AppliedIndex = %d, want %d", fetched.AppliedIndex, manifestA.AppliedIndex)
	}
	claimKey := repoA.key(backupClaimsDir, manifestA.BackupID+".json")
	if _, ok := client.objects[claimKey]; !ok {
		t.Fatal("committed claim was removed")
	}
	for key := range client.objects {
		if strings.Contains(key, "/staging/"+manifestA.BackupID+"/") &&
			!strings.Contains(key, "/"+claim.AttemptID+"/") {
			t.Fatalf("staging key does not contain owning attempt: %q", key)
		}
	}
}

func TestS3BackupRepositoryFailedOwnerCleansClaimAndArtifacts(t *testing.T) {
	client := newFakeS3API()
	repo, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	checkpointDir, manifest := createManifestFixture(t)
	repo.beforeCommit = func(s3BackupClaim) error { return errors.New("stop before marker") }
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err == nil {
		t.Fatal("Publish returned nil")
	}
	for key := range client.objects {
		if strings.Contains(key, "/"+manifest.BackupID+"/") ||
			key == repo.key(backupClaimsDir, manifest.BackupID+".json") {
			t.Fatalf("failed owner left object %q", key)
		}
	}
}

func TestS3BackupRepositoryReconcilesStoredMarkerAfterPutError(t *testing.T) {
	client := newFakeS3API()
	repo, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	checkpointDir, manifest := createManifestFixture(t)
	markerKey := repo.key(backupCommittedDir, manifest.BackupID, backupCommitMarker)
	client.storeThenErrorKey = markerKey

	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatalf("Publish did not reconcile stored marker: %v", err)
	}
	markerObject, ok := client.objects[markerKey]
	if !ok {
		t.Fatal("stored marker disappeared during reconciliation")
	}
	var marker s3CommitMarker
	if err := json.Unmarshal(markerObject.data, &marker); err != nil {
		t.Fatalf("decode marker: %v", err)
	}
	if marker.BackupID != manifest.BackupID || marker.AttemptID == "" || marker.ManifestSHA256 == "" {
		t.Fatalf("marker does not identify publication: %+v", marker)
	}
	claimKey := repo.key(backupClaimsDir, manifest.BackupID+".json")
	if _, ok := client.objects[claimKey]; !ok {
		t.Fatal("reconciled Publish removed claim")
	}
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, filepath.Join(realTempDir(t), "restore")); err != nil {
		t.Fatal(err)
	}
}

func TestS3BackupRepositoryRetainsArtifactsWhenCommitResultIsIndeterminate(t *testing.T) {
	client := newFakeS3API()
	repo, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	checkpointDir, manifest := createManifestFixture(t)
	markerKey := repo.key(backupCommittedDir, manifest.BackupID, backupCommitMarker)
	client.putErrorThenGetErrorKey = markerKey

	err = repo.Publish(context.Background(), checkpointDir, manifest)
	if err == nil || !strings.Contains(err.Error(), "indeterminate commit") {
		t.Fatalf("Publish error = %v, want indeterminate commit", err)
	}
	if len(client.deleteBatches) != 0 {
		t.Fatalf("indeterminate Publish performed destructive cleanup: %v", client.deleteBatches)
	}
	claimKey := repo.key(backupClaimsDir, manifest.BackupID+".json")
	finalManifestKey := repo.key(backupCommittedDir, manifest.BackupID, backupManifestFile)
	for _, key := range []string{claimKey, finalManifestKey} {
		if _, ok := client.objects[key]; !ok {
			t.Fatalf("indeterminate Publish removed %q", key)
		}
	}
	stagePrefix := repo.key(backupStagingDir, manifest.BackupID) + "/"
	foundStage := false
	for key := range client.objects {
		foundStage = foundStage || strings.HasPrefix(key, stagePrefix)
	}
	if !foundStage {
		t.Fatal("indeterminate Publish removed staging attempt")
	}
}

func TestS3BackupRepositoryDeleteRemovesMarkerLast(t *testing.T) {
	client := newFakeS3API()
	repo, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	checkpointDir, manifest := createManifestFixture(t)
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}
	client.deleteBatches = nil
	client.ops = nil
	if err := repo.Delete(context.Background(), manifest.BackupID); err != nil {
		t.Fatal(err)
	}
	markerKey := repo.key(backupCommittedDir, manifest.BackupID, backupCommitMarker)
	claimKey := repo.key(backupClaimsDir, manifest.BackupID+".json")
	manifestKey := repo.key(backupCommittedDir, manifest.BackupID, backupManifestFile)
	if len(client.deleteBatches) < 2 {
		t.Fatalf("delete batches = %v", client.deleteBatches)
	}
	first := client.deleteBatches[0]
	for _, key := range first {
		if key == markerKey || key == claimKey || key == manifestKey {
			t.Fatalf("first delete batch = %v, must delete artifacts before marker/claim/manifest", first)
		}
	}
	if len(first) == 0 {
		t.Fatalf("first delete batch = %v, want artifact keys", first)
	}
	last := client.deleteBatches[len(client.deleteBatches)-1]
	if len(last) != 1 || last[0] != markerKey {
		t.Fatalf("last delete batch = %v, want marker only", last)
	}
	positions := map[string]int{}
	for i, batch := range client.deleteBatches {
		for _, key := range batch {
			positions[key] = i
		}
	}
	if positions[claimKey] >= positions[markerKey] {
		t.Fatalf("claim delete position = %d, marker position = %d", positions[claimKey], positions[markerKey])
	}
	if positions[manifestKey] >= positions[markerKey] {
		t.Fatalf("manifest delete position = %d, marker position = %d", positions[manifestKey], positions[markerKey])
	}
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, filepath.Join(realTempDir(t), "restore")); err == nil {
		t.Fatal("Fetch saw backup after Delete")
	}
}

func TestS3BackupRepositoryDeleteArtifactFailureKeepsCommittedMarkerVisible(t *testing.T) {
	client := newFakeS3API()
	repo, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	checkpointDir, manifest := createManifestFixture(t)
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}
	markerKey := repo.key(backupCommittedDir, manifest.BackupID, backupCommitMarker)
	fileKey := repo.key(backupCommittedDir, manifest.BackupID, backupFilesDir, manifest.Files[0].Path)
	client.deleteBatches = nil
	client.deleteErrors[fileKey] = "injected artifact delete failure"

	if err := repo.Delete(context.Background(), manifest.BackupID); err == nil {
		t.Fatal("Delete ignored artifact delete failure")
	}
	if _, ok := client.objects[markerKey]; !ok {
		t.Fatal("Delete removed commit marker before artifact deletion succeeded")
	}
	backups, err := repo.ListCommitted(context.Background())
	if err != nil {
		t.Fatalf("ListCommitted: %v", err)
	}
	if len(backups) != 1 || backups[0].ID != manifest.BackupID {
		t.Fatalf("ListCommitted = %+v, want failed delete backup visible", backups)
	}
	for _, batch := range client.deleteBatches {
		for _, key := range batch {
			if key == markerKey {
				t.Fatalf("Delete attempted marker in failed artifact run: %v", client.deleteBatches)
			}
		}
	}
}

func TestS3BackupRepositoryJanitorIsStrictlyStagingOnlyAndRespectsAnyClaim(t *testing.T) {
	client := newFakeS3API()
	repo, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	checkpointDir, manifest := createManifestFixture(t)
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}
	claimKey := repo.key(backupClaimsDir, manifest.BackupID+".json")
	if err := repo.DeleteStagingOlderThan(context.Background(), time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, ok := client.objects[claimKey]; !ok {
		t.Fatal("janitor deleted committed claim")
	}

	staleID := "stale"
	staleAttempt := strings.Repeat("a", 32)
	staleClaim := s3BackupClaim{
		BackupID:       staleID,
		AttemptID:      staleAttempt,
		ManifestSHA256: strings.Repeat("b", 64),
		CreatedAt:      time.Now().UTC().Add(-48 * time.Hour),
	}
	claimData, err := json.Marshal(staleClaim)
	if err != nil {
		t.Fatal(err)
	}
	staleClaimKey := repo.key(backupClaimsDir, staleID+".json")
	staleStageKey := repo.key(backupStagingDir, staleID, staleAttempt, backupManifestFile)
	staleFinalKey := repo.key(backupCommittedDir, staleID, backupManifestFile)
	staleMarkerKey := repo.key(backupCommittedDir, staleID, backupCommitMarker)
	old := time.Now().UTC().Add(-48 * time.Hour)
	client.objects[staleClaimKey] = fakeS3Object{data: claimData, lastModified: old}
	client.objects[staleStageKey] = fakeS3Object{data: []byte("{}"), lastModified: old}
	client.objects[staleFinalKey] = fakeS3Object{data: []byte("final"), lastModified: old}
	client.objects[staleMarkerKey] = fakeS3Object{data: []byte("marker"), lastModified: old}
	if err := repo.DeleteStagingOlderThan(context.Background(), time.Now().UTC().Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{staleClaimKey, staleStageKey, staleFinalKey, staleMarkerKey} {
		if _, ok := client.objects[key]; !ok {
			t.Fatalf("janitor deleted claimed or non-staging object %q", key)
		}
	}

	orphanID := "orphan"
	orphanAttempt := strings.Repeat("c", 32)
	orphanStageKey := repo.key(backupStagingDir, orphanID, orphanAttempt, backupManifestFile)
	client.objects[orphanStageKey] = fakeS3Object{data: []byte("{}"), lastModified: old}
	if err := repo.DeleteStagingOlderThan(context.Background(), time.Now().UTC().Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, ok := client.objects[orphanStageKey]; ok {
		t.Fatal("janitor retained old unclaimed staging attempt")
	}
}

func TestS3BackupRepositoryJanitorDoesNotDeleteActiveClaim(t *testing.T) {
	client := newFakeS3API()
	repo, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	checkpointDir, manifest := createManifestFixture(t)
	claimed := make(chan s3BackupClaim, 1)
	release := make(chan struct{})
	repo.afterClaim = func(claim s3BackupClaim) error {
		claimed <- claim
		<-release
		return nil
	}
	publishErr := make(chan error, 1)
	go func() { publishErr <- repo.Publish(context.Background(), checkpointDir, manifest) }()
	<-claimed
	janitor, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	if err := janitor.DeleteStagingOlderThan(context.Background(), time.Now().UTC().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, ok := client.objects[repo.key(backupClaimsDir, manifest.BackupID+".json")]; !ok {
		t.Fatal("janitor deleted active non-stale claim")
	}
	close(release)
	if err := <-publishErr; err != nil {
		t.Fatal(err)
	}
}

func TestS3BackupRepositoryJanitorRechecksClaimAfterStagingList(t *testing.T) {
	client := newFakeS3API()
	repo, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	backupID := "replacement"
	oldAttempt := strings.Repeat("d", 32)
	newAttempt := strings.Repeat("e", 32)
	old := time.Now().UTC().Add(-48 * time.Hour)
	stageKey := repo.key(backupStagingDir, backupID, oldAttempt, backupManifestFile)
	finalKey := repo.key(backupCommittedDir, backupID, backupManifestFile)
	claimKey := repo.key(backupClaimsDir, backupID+".json")
	client.objects[stageKey] = fakeS3Object{data: []byte("{}"), lastModified: old}
	client.objects[finalKey] = fakeS3Object{data: []byte("final"), lastModified: old}
	replacement := s3BackupClaim{
		BackupID:       backupID,
		AttemptID:      newAttempt,
		ManifestSHA256: strings.Repeat("f", 64),
		CreatedAt:      time.Now().UTC(),
	}
	claimData, err := json.Marshal(replacement)
	if err != nil {
		t.Fatal(err)
	}
	client.afterList[repo.key(backupStagingDir)+"/"] = func() {
		client.mu.Lock()
		defer client.mu.Unlock()
		client.objects[claimKey] = fakeS3Object{data: claimData, lastModified: time.Now().UTC()}
	}

	if err := repo.DeleteStagingOlderThan(context.Background(), time.Now().UTC().Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{stageKey, finalKey, claimKey} {
		if _, ok := client.objects[key]; !ok {
			t.Fatalf("janitor deleted object after replacement claim appeared: %q", key)
		}
	}
}

func TestS3BackupRepositoryJanitorBetweenOwnershipCheckAndCommitIsHarmless(t *testing.T) {
	client := newFakeS3API()
	publisher, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	janitor, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	checkpointDir, manifest := createManifestFixture(t)
	checked := make(chan s3BackupClaim, 1)
	release := make(chan struct{})
	old := time.Now().UTC().Add(-48 * time.Hour)
	publisher.beforeMarker = func(claim s3BackupClaim) error {
		client.mu.Lock()
		stagePrefix := publisher.key(backupStagingDir, manifest.BackupID, claim.AttemptID) + "/"
		for key, object := range client.objects {
			if strings.HasPrefix(key, stagePrefix) {
				object.lastModified = old
				client.objects[key] = object
			}
		}
		client.mu.Unlock()
		checked <- claim
		<-release
		return nil
	}
	publishErr := make(chan error, 1)
	go func() { publishErr <- publisher.Publish(context.Background(), checkpointDir, manifest) }()
	claim := <-checked

	if err := janitor.DeleteStagingOlderThan(context.Background(), time.Now().UTC().Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	claimKey := publisher.key(backupClaimsDir, manifest.BackupID+".json")
	finalManifestKey := publisher.key(backupCommittedDir, manifest.BackupID, backupManifestFile)
	stageManifestKey := publisher.key(backupStagingDir, manifest.BackupID, claim.AttemptID, backupManifestFile)
	for _, key := range []string{claimKey, finalManifestKey, stageManifestKey} {
		if _, ok := client.objects[key]; !ok {
			t.Fatalf("janitor deleted publication object %q", key)
		}
	}
	close(release)
	if err := <-publishErr; err != nil {
		t.Fatal(err)
	}
	finalBefore := append([]byte(nil), client.objects[finalManifestKey].data...)
	different := *manifest
	different.AppliedIndex++
	if err := janitor.Publish(context.Background(), checkpointDir, &different); err == nil {
		t.Fatal("different publisher overwrote committed backup after janitor interleaving")
	}
	if !bytes.Equal(client.objects[finalManifestKey].data, finalBefore) {
		t.Fatal("final manifest changed after COMMITTED")
	}
	if _, err := janitor.Fetch(context.Background(), manifest.BackupID, filepath.Join(realTempDir(t), "restore")); err != nil {
		t.Fatal(err)
	}
}

func TestS3BackupRepositoryRejectsRepeatedContinuationToken(t *testing.T) {
	client := newFakeS3API()
	client.repeatToken = true
	repo, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = repo.listAll(ctx, repo.key(backupCommittedDir)+"/")
	if err == nil || !strings.Contains(err.Error(), "repeated continuation token") {
		t.Fatalf("listAll error = %v, want repeated continuation token", err)
	}
}

func TestS3BackupRepositoryClosesBodiesOnAbnormalGetResults(t *testing.T) {
	client := newFakeS3API()
	repo, err := newS3BackupRepository(client, "bucket", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	nilKey := repo.key("nil")
	client.nilNilGetKey = nilKey
	if _, err := repo.getBytes(context.Background(), nilKey); err == nil {
		t.Fatal("getBytes accepted nil output")
	}
	errorKey := repo.key("error")
	client.errorBodyKey = errorKey
	if _, err := repo.getBytes(context.Background(), errorKey); err == nil {
		t.Fatal("getBytes accepted output-with-error")
	}
	if len(client.bodies) == 0 || !client.bodies[len(client.bodies)-1].closed {
		t.Fatal("getBytes did not close body returned with error")
	}

	checkpointDir, manifest := createManifestFixture(t)
	client.errorBodyKey = ""
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}
	fileKey := repo.key(backupCommittedDir, manifest.BackupID, backupFilesDir, manifest.Files[0].Path)
	client.errorBodyKey = fileKey
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, filepath.Join(realTempDir(t), "restore")); err == nil {
		t.Fatal("Fetch accepted output-with-body-and-error")
	}
	if !client.bodies[len(client.bodies)-1].closed {
		t.Fatal("Fetch did not close body returned with error")
	}
}
