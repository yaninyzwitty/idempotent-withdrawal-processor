CREATE TABLE withdrawals (
    id VARCHAR(36) PRIMARY KEY,
    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
    user_id VARCHAR(36) NOT NULL,
    asset VARCHAR(50) NOT NULL,
    amount VARCHAR(78) NOT NULL,
    destination_addr VARCHAR(255) NOT NULL,
    network VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    error_message TEXT,
    tx_hash VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,
    processing_version BIGINT NOT NULL DEFAULT 1
);

CREATE INDEX idx_withdrawals_idempotency_key ON withdrawals(idempotency_key);
CREATE INDEX idx_withdrawals_user_id ON withdrawals(user_id);
CREATE INDEX idx_withdrawals_status ON withdrawals(status);
CREATE INDEX idx_withdrawals_created_at ON withdrawals(created_at);

CREATE TABLE idempotency_keys (
    key VARCHAR(255) PRIMARY KEY,
    withdrawal_id VARCHAR(36),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fk_idempotency_withdrawal FOREIGN KEY (withdrawal_id) REFERENCES withdrawals(id) ON DELETE CASCADE
);

CREATE INDEX idx_idempotency_keys_expires_at ON idempotency_keys(expires_at);

CREATE TABLE processing_locks (
    withdrawal_id VARCHAR(36) PRIMARY KEY,
    locked_by VARCHAR(36),
    locked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fk_lock_withdrawal FOREIGN KEY (withdrawal_id) REFERENCES withdrawals(id) ON DELETE CASCADE
);

CREATE INDEX idx_processing_locks_expires_at ON processing_locks(expires_at);
