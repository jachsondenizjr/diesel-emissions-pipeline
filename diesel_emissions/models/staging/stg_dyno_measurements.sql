-- models/staging/stg_dyno_measurements.sql
-- =============================================================
-- Camada Silver — staging
-- Limpa e padroniza os dados brutos da tabela dyno_measurements
-- Fonte: diesel_raw.dyno_measurements (camada bronze)
-- =============================================================

with source as (

    select * from {{ source('diesel_raw', 'dyno_measurements') }}

),

cleaned as (

    select
        -- Identifiers
        test_id,
        engine_id,
        test_cell,
        timestamp,

        -- WHSC cycle info
        whsc_mode,
        mode_speed_pct,
        mode_torque_pct,
        mode_weight_factor,
        sample_num,

        -- Engine operating conditions (rounded for consistency)
        round(rpm, 1)                as rpm,
        round(torque_nm, 1)          as torque_nm,
        round(power_kw, 2)           as power_kw,
        round(fuel_flow_g_h, 1)      as fuel_flow_g_h,
        round(boost_pressure_bar, 2) as boost_pressure_bar,
        round(lambda, 3)             as lambda,

        -- Emissions (g/kWh)
        round(nox_g_kwh, 4)          as nox_g_kwh,
        round(co_g_kwh, 4)           as co_g_kwh,
        round(co2_g_kwh, 2)          as co2_g_kwh,
        round(hc_g_kwh, 4)           as hc_g_kwh,
        round(pm_g_kwh, 5)           as pm_g_kwh,

        -- Temperatures (°C)
        round(coolant_temp_c, 1)     as coolant_temp_c,
        round(exhaust_temp_c, 1)     as exhaust_temp_c,
        round(oil_temp_c, 1)         as oil_temp_c,
        round(intake_air_temp_c, 1)  as intake_air_temp_c,

        -- Compliance flags — EPA Tier 4 (calculados a partir dos limites em dbt_project.yml)
        nox_g_kwh <= {{ var('epa_tier4_nox_limit') }}  as epa_tier4_nox_pass,
        co_g_kwh  <= {{ var('epa_tier4_co_limit') }}   as epa_tier4_co_pass,
        hc_g_kwh  <= {{ var('epa_tier4_hc_limit') }}   as epa_tier4_hc_pass,
        pm_g_kwh  <= {{ var('epa_tier4_pm_limit') }}   as epa_tier4_pm_pass,

        -- Compliance flags — Euro VI
        nox_g_kwh <= {{ var('euro6_nox_limit') }}      as euro6_nox_pass,
        co_g_kwh  <= {{ var('euro6_co_limit') }}       as euro6_co_pass,
        hc_g_kwh  <= {{ var('euro6_hc_limit') }}       as euro6_hc_pass,
        pm_g_kwh  <= {{ var('euro6_pm_limit') }}       as euro6_pm_pass,

        -- Derived: overall compliance (all pollutants must pass)
        (nox_g_kwh <= {{ var('epa_tier4_nox_limit') }}
            and co_g_kwh <= {{ var('epa_tier4_co_limit') }}
            and hc_g_kwh <= {{ var('epa_tier4_hc_limit') }}
            and pm_g_kwh <= {{ var('epa_tier4_pm_limit') }}) as epa_tier4_overall_pass,

        (nox_g_kwh <= {{ var('euro6_nox_limit') }}
            and co_g_kwh <= {{ var('euro6_co_limit') }}
            and hc_g_kwh <= {{ var('euro6_hc_limit') }}
            and pm_g_kwh <= {{ var('euro6_pm_limit') }})     as euro6_overall_pass,

        -- Derived: load category based on torque %
        case
            when mode_torque_pct = 0   then 'idle'
            when mode_torque_pct <= 25 then 'low_load'
            when mode_torque_pct <= 50 then 'mid_load'
            when mode_torque_pct <= 75 then 'high_load'
            else 'full_load'
        end                          as load_category,

        -- Derived: BSFC (brake specific fuel consumption) in g/kWh
        case
            when power_kw > 0
            then round(fuel_flow_g_h / power_kw, 1)
            else null
        end                          as bsfc_g_kwh

    from source
    where
        -- Remove rows with critical nulls
        test_id       is not null
        and engine_id is not null
        and timestamp is not null
        and rpm       is not null
        and nox_g_kwh is not null

)

select * from cleaned