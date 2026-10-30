-- fact_machine_events -- TESTIKANNAN kopio tuotannon kohdetaulusta.
--
-- Tarkoitus: mtconnect_data_dag voidaan ajaa testiympäristössä loppuun asti, kirjoitus
-- mukaan lukien. DAG ei luo taulua itse, ja tuotannossa kanta omistaa skeemansa --
-- tätä tiedostoa EI ajeta tuotantoon. warehouse-kontti ajaa sen ensikäynnistyksessä
-- (docker-entrypoint-initdb.d).
--
-- Lähde: tuotannon "\d public.fact_machine_events" 7.10.2026. Sarakkeet, tyypit,
-- pääavain ja erp_id:n uniikkius ovat samat kuin tuotannossa.
--
-- Ero tuotantoon: VIERAITA AVAIMIA EI OLE, koska kehityskannassa ei ole dim-tauluja
-- (dim_date, dim_time, dim_source, dim_event_type, dim_workcenter ...). Kehityksessä
-- ei siis huomata arvoa, joka puuttuu dim-taulusta; se on tarkistettava tuotannossa
-- ennen koeajoa.
--
-- Skriptin voi ajaa uudelleen: olemassa olevaan tauluun ei kosketa.

CREATE TABLE IF NOT EXISTS public.fact_machine_events (
    -- Tapahtuman luokittelu ja alkuperä
    event_type_id              bigint,            -- 8, 10, 11, 14, 16, 17 (dim_event_type)
    source_id                  integer,           -- 1 STAMPING, 2 MACHINE API, 3 SENSOR (dim_source)
    -- ERP-leimauksen kentät: vain lähde 1 täyttää, mtconnect_data_dag jättää tyhjiksi
    workphase_id               bigint,
    work_number                text,
    work_id                    bigint,
    workcenter_id              bigint,            -- kone (dim_workcenter)
    article_id                 bigint,
    user_id                    bigint,
    program_name               text,              -- NC-ohjelman nimi
    -- Aika: kokonaisluvut ovat PAIKALLISTA aikaa (avaimet dim_date / dim_time),
    -- start_at / end_at tapahtuman todellinen hetki
    start_date                 integer,           -- YYYYMMDD
    end_date                   integer,           -- YYYYMMDD
    start_time                 integer,           -- HHMMSS
    end_time                   integer,           -- HHMMSS
    start_at                   timestamp with time zone,
    end_at                     timestamp with time zone,
    duration_min               double precision,  -- end_at - start_at
    -- Kappalekohtaiset ajat ja määrät
    unit_time_min              double precision,
    unit_cycle_time_min        double precision,
    unit_processing_time_min   double precision,
    produced_qty_stamping      integer,           -- lähde 1
    produced_qty_machine       integer,           -- koneen kappalelaskurista
    rejected_qty_stamping      integer,           -- lähde 1
    part_counter               integer,
    -- Ajan jakautuminen tiloittain: minuutit ja osuus 0-1
    efficient_production_min   double precision,
    efficient_production_pct   double precision,
    inefficient_production_min double precision,
    inefficient_production_pct double precision,
    cutting_time_min           double precision,
    non_cutting_time_min       double precision,
    feed_override              double precision,
    spindle_override           double precision,
    machine_active_min         double precision,
    machine_active_pct         double precision,
    setup_time_min             double precision,
    setup_time_pct             double precision,
    part_change_time_min       double precision,
    part_change_time_pct       double precision,
    undefined_idle_min         double precision,
    undefined_idle_pct         double precision,
    idle_sum_min               double precision,
    idle_sum_pct               double precision,
    power_off_min              double precision,
    power_off_pct              double precision,
    other_min                  double precision,
    other_pct                  double precision,
    exception_min              double precision,
    exception_pct              double precision,
    exception_id               bigint,
    power_consumption_kwh      double precision,
    notes                      text,
    -- Kanta generoi tunnuksen; DAG ei täytä sitä (siksi replace_day poistaa muilla kentillä)
    machine_event_id           bigserial NOT NULL,
    erp_id                     bigint,            -- lähteen 1 rivin tunnus ERP:ssä
    CONSTRAINT fact_machine_events_pkey PRIMARY KEY (machine_event_id),
    CONSTRAINT erp_id_uniq UNIQUE (erp_id)
);
