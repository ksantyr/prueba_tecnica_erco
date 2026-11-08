CREATE ROLE erco_escritura LOGIN PASSWORD 'erco_escritura';
CREATE ROLE erco_lectura LOGIN PASSWORD 'erco_lectura';

CREATE DATABASE erco OWNER erco_escritura;

ALTER DATABASE erco SET timezone TO 'America/Bogota';

REVOKE ALL ON DATABASE erco FROM PUBLIC;
GRANT CONNECT ON DATABASE erco TO erco_lectura;

\connect erco

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA public TO erco_escritura;
GRANT USAGE ON SCHEMA public TO erco_lectura;

SET ROLE erco_escritura;

CREATE TABLE public.services (
	id_services serial4 NOT NULL,
	created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	created_by varchar(50) NOT NULL,
	updated_at timestamp NULL,
	updated_by varchar(50) NULL,
	id_service int4 NOT NULL,
	id_market int4 NOT NULL,
	cdi int4 NULL,
	voltage_level int4 NOT NULL,
	source_file varchar(255) NULL,
	CONSTRAINT services_pk PRIMARY KEY (id_services),
	CONSTRAINT ck_services_voltage_level CHECK (voltage_level BETWEEN 1 AND 4)
);

CREATE UNIQUE INDEX uq_services_id_service ON public.services USING btree (id_service);
CREATE INDEX ix_services_id_market_voltage_level ON public.services USING btree (id_market, voltage_level);

CREATE OR REPLACE VIEW public.vw_services AS
SELECT
    id_service,
    id_market,
    voltage_level,
    cdi
FROM public.services;

GRANT SELECT ON TABLE public.vw_services TO erco_lectura;

CREATE TABLE public.records (
	id_records serial4 NOT NULL,
	created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	created_by varchar(50) NOT NULL,
	updated_at timestamp NULL,
	updated_by varchar(50) NULL,
	id_record int4 NOT NULL,
	id_service int4 NOT NULL,
	record_timestamp timestamp NOT NULL,
	source_file varchar(255) NULL,
	CONSTRAINT records_pk PRIMARY KEY (id_records),
	CONSTRAINT fk_records_services FOREIGN KEY (id_service)
		REFERENCES public.services (id_service) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE UNIQUE INDEX uq_records_id_record ON public.records USING btree (id_record);
CREATE UNIQUE INDEX uq_records_id_service_record_timestamp ON public.records USING btree (id_service, record_timestamp);
CREATE INDEX ix_records_record_timestamp ON public.records USING btree (record_timestamp);

CREATE OR REPLACE VIEW public.vw_records AS
SELECT
    id_record,
    id_service,
    record_timestamp
FROM public.records;

GRANT SELECT ON TABLE public.vw_records TO erco_lectura;

CREATE TABLE public.consumption (
	id_consumption serial4 NOT NULL,
	created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	created_by varchar(50) NOT NULL,
	updated_at timestamp NULL,
	updated_by varchar(50) NULL,
	id_record int4 NOT NULL,
	value numeric(18, 6) NOT NULL,
	source_file varchar(255) NULL,
	CONSTRAINT consumption_pk PRIMARY KEY (id_consumption),
	CONSTRAINT fk_consumption_records FOREIGN KEY (id_record)
		REFERENCES public.records (id_record) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE UNIQUE INDEX uq_consumption_id_record ON public.consumption USING btree (id_record);

CREATE OR REPLACE VIEW public.vw_consumption AS
SELECT
    id_record,
    value
FROM public.consumption;

GRANT SELECT ON TABLE public.vw_consumption TO erco_lectura;

CREATE TABLE public.injection (
	id_injection serial4 NOT NULL,
	created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	created_by varchar(50) NOT NULL,
	updated_at timestamp NULL,
	updated_by varchar(50) NULL,
	id_record int4 NOT NULL,
	value numeric(18, 6) NOT NULL,
	source_file varchar(255) NULL,
	CONSTRAINT injection_pk PRIMARY KEY (id_injection),
	CONSTRAINT fk_injection_records FOREIGN KEY (id_record)
		REFERENCES public.records (id_record) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE UNIQUE INDEX uq_injection_id_record ON public.injection USING btree (id_record);

CREATE OR REPLACE VIEW public.vw_injection AS
SELECT
    id_record,
    value
FROM public.injection;

GRANT SELECT ON TABLE public.vw_injection TO erco_lectura;

CREATE TABLE public.tariffs (
	id_tariffs serial4 NOT NULL,
	created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	created_by varchar(50) NOT NULL,
	updated_at timestamp NULL,
	updated_by varchar(50) NULL,
	id_market int4 NOT NULL,
	voltage_level int4 NOT NULL,
	cdi int4 NULL,
	g numeric(18, 8) NULL,
	t numeric(18, 8) NULL,
	d numeric(18, 8) NULL,
	r numeric(18, 8) NULL,
	c numeric(18, 8) NULL,
	p numeric(18, 8) NULL,
	cu numeric(18, 8) NULL,
	source_file varchar(255) NULL,
	CONSTRAINT tariffs_pk PRIMARY KEY (id_tariffs),
	CONSTRAINT ck_tariffs_voltage_level CHECK (voltage_level BETWEEN 1 AND 4),
	CONSTRAINT ck_tariffs_cdi_niveles_2_3 CHECK (voltage_level NOT IN (2, 3) OR cdi IS NULL)
);

CREATE UNIQUE INDEX uq_tariffs_id_market_voltage_level_cdi ON public.tariffs USING btree (id_market, voltage_level, cdi) NULLS NOT DISTINCT;

CREATE OR REPLACE VIEW public.vw_tariffs AS
SELECT
    id_market,
    voltage_level,
    cdi,
    g,
    t,
    d,
    r,
    c,
    p,
    cu
FROM public.tariffs;

GRANT SELECT ON TABLE public.vw_tariffs TO erco_lectura;

CREATE TABLE public.xm_data_hourly_per_agent (
	id_xm_data_hourly_per_agent serial4 NOT NULL,
	created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	created_by varchar(50) NOT NULL,
	updated_at timestamp NULL,
	updated_by varchar(50) NULL,
	record_timestamp timestamp NOT NULL,
	value numeric(20, 10) NOT NULL,
	source_file varchar(255) NULL,
	CONSTRAINT xm_data_hourly_per_agent_pk PRIMARY KEY (id_xm_data_hourly_per_agent)
);

CREATE UNIQUE INDEX uq_xm_data_hourly_per_agent_record_timestamp ON public.xm_data_hourly_per_agent USING btree (record_timestamp);

CREATE OR REPLACE VIEW public.vw_xm_data_hourly_per_agent AS
SELECT
    record_timestamp,
    value
FROM public.xm_data_hourly_per_agent;

GRANT SELECT ON TABLE public.vw_xm_data_hourly_per_agent TO erco_lectura;

RESET ROLE;
