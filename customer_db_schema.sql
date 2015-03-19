/**
 * Database Name: customer.db
 * Purpose: 
 *   Contains system info collection result from customers.
 */

create table systems (
    id                  serial primary key not null unique,
    customer_name       text,
    unify_name          text,
    system_version      text not null,
    upgrade_to		    text,
    rmg_count           text,
    config_node_count   text,
    expect_node_count   text,
    total_capacity      text,
    used_capacity       text,
    hardware_mode       text,
    mds_replica_mode    text,
    remote_mds          text,
    syr_report          text,
    tenant_count        text,
    subtenant_count     text,
    uid_count           text,
    mr_object_count     text,
    object_count        text,
    average_size        text,
    real_size           text,
    metadata_size       text,
    total_size          text,
    last_collect_time   text
    );

create table rmgs (
    id                  serial primary key not null unique,
    customer_id         integer,
    rmg_name            text,
    rmg_node_count      integer
    );

create table clusters (
    id                  serial primary key not null unique,
    customer_id         integer,
    rmg_id              integer,
    segment_node_count  integer,
    clustername         text,
    sitename            text,
    location            text,
    master_node         text,
    replica_clustername text
    );

create table nodes (
    id                  serial primary key not null unique,
    customer_id         integer,
    rmg_id              integer,
    segment_id          integer,
    hostname            text,
    curversion          text,
    diskratio           text,           -- Disk ratio for MDS/SS allocation
    ss_action           text,           -- SS actions. NONE, Compression, Deduplication, Compression&Deduplication
    access_method       text,
    multi_subtenant     text,
    web_service         text,
    cas_status          text,
    node_replaced       text
    );

create table disks (
    id                  serial primary key not null unique,
    customer_id         integer,
    fsuuid              text,
    unrecoverobj        text,
    impactobj           text,
    retriedtimes        text,
    status              text
    );

create table networkcfgs (
    id                  serial primary key not null unique,
    customer_id         integer,
    segment_name        text,
    config              text
    );

create table policycfgs (
    id                  serial primary key not null unique,
    customer_id         integer,
    tenant_name         text,
    policy_selector     text,
    policy_spec         text
    );
