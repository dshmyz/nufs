{{- define "nufs.labels" -}}
app.kubernetes.io/name: nufs
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{- define "nufs.metad.labels" -}}
{{ include "nufs.labels" . }}
app.kubernetes.io/component: metad
{{- end }}

{{- define "nufs.datanode.labels" -}}
{{ include "nufs.labels" . }}
app.kubernetes.io/component: datanode
{{- end }}

{{- define "nufs.s3gateway.labels" -}}
{{ include "nufs.labels" . }}
app.kubernetes.io/component: s3gateway
{{- end }}
