CREATE TABLE integration_pair(
    id SERIAL PRIMARY KEY NOT NULL,
    source TEXT NOT NULL,
    destination TEXT NOT NULL,
    source_config JSONB NOT NULL DEFAULT '{}'::JSONB,
    destination_config JSONB NOT NULL DEFAULT '{}'::JSONB,
    source_orgunits_synced BOOLEAN NOT NULL DEFAULT FALSE,
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE sync_datasets (
    id SERIAL PRIMARY KEY NOT NULL,
    integration_pair_id INTEGER NOT NULL REFERENCES integration_pair(id),
    dataset_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    reporting_frequency TEXT NOT NULL CHECK (reporting_frequency IN ('Daily', 'Weekly', 'Monthly', 'Quarterly', 'Yearly', 'FinancialJuly')),
    include_deleted BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    last_sync TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- last successful sync
    created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    
);

CREATE TABLE orgunits(
    id SERIAL PRIMARY KEY NOT NULL,
    integration_pair_id INTEGER NOT NULL REFERENCES integration_pair(id),
    dhis2_name TEXT NOT NULL,
    dhis2_id VARCHAR(12) NOT NULL DEFAULT '',
    dhis2_path TEXT NOT NULL DEFAULT '',
    dhis2_parent TEXT NOT NULL DEFAULT '',
    dhis2_level INTEGER NOT NULL,
    priority INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO integration_pair(source, destination, source_config, destination_config, is_active)
VALUES (
        'hmis', 'repo',
        '{"use_pat":false, "username":"admin", "password":"xxxx", "api_url": "https://hmis.health.go.ug/api",
          "pat":"", "ous_synced": "false", "datasets_synced": "false"}'::JSONB,
        '{"use_pat":false, "username":"admin", "password": "xxx:", "api_url": "https://hmis-repo.health.go.ug/api", "pat":""}'::JSONB,
        TRUE
        );