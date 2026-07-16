-- NUFS Admin 数据库表结构
-- 用于动态集群管理

CREATE TABLE IF NOT EXISTS clusters (
    id            VARCHAR(64)  PRIMARY KEY,
    region        VARCHAR(32)  NOT NULL,
    metad_ops_url VARCHAR(256) NOT NULL,
    description   VARCHAR(256),
    source        ENUM('static', 'dynamic') NOT NULL DEFAULT 'dynamic',
    created_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    INDEX idx_source (source),
    INDEX idx_region (region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 集群变更审计日志
CREATE TABLE IF NOT EXISTS cluster_audit_log (
    id          BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cluster_id  VARCHAR(64)  NOT NULL,
    action      ENUM('add', 'remove', 'update') NOT NULL,
    operator    VARCHAR(64)  NOT NULL,
    detail      TEXT,
    created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_cluster (cluster_id),
    INDEX idx_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;