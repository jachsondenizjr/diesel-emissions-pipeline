-- models/marts/engine_performance.sql
-- =============================================================
-- Camada Gold — mart de performance do motor
-- Agrega métricas de eficiência por motor, faixa de RPM e carga
-- Fonte: stg_dyno_measurements (camada silver)
-- =============================================================

with staging as (

    select * from {{ ref('stg_dyno_measurements') }}

),

-- Métricas por motor e categoria de carga
performance as (

    select
        engine_id,
        test_cell,
        load_category,
        whsc_mode,
        mode_speed_pct,
        mode_torque_pct,

        -- Médias de operação
        round(avg(rpm), 1)               as avg_rpm,
        round(avg(torque_nm), 1)         as avg_torque_nm,
        round(avg(power_kw), 2)          as avg_power_kw,
        round(max(power_kw), 2)          as max_power_kw,

        -- Eficiência
        round(avg(bsfc_g_kwh), 1)        as avg_bsfc_g_kwh,
        round(min(bsfc_g_kwh), 1)        as best_bsfc_g_kwh,
        round(avg(fuel_flow_g_h), 1)     as avg_fuel_flow_g_h,

        -- Temperaturas médias
        round(avg(exhaust_temp_c), 1)    as avg_exhaust_temp_c,
        round(avg(coolant_temp_c), 1)    as avg_coolant_temp_c,
        round(avg(oil_temp_c), 1)        as avg_oil_temp_c,

        -- Emissões médias por ponto de operação
        round(avg(nox_g_kwh), 4)         as avg_nox_g_kwh,
        round(avg(co2_g_kwh), 2)         as avg_co2_g_kwh,
        round(avg(pm_g_kwh), 5)          as avg_pm_g_kwh,

        -- NOx vs potência (índice de eficiência ambiental)
        round(
            avg(nox_g_kwh) / nullif(avg(power_kw), 0), 6
        )                                as nox_per_kw,

        -- Turbocharger
        round(avg(boost_pressure_bar), 3) as avg_boost_bar,
        round(avg(lambda), 3)             as avg_lambda,

        count(*)                          as sample_count

    from staging
    where power_kw > 0  -- exclui idle para métricas de eficiência
    group by
        engine_id,
        test_cell,
        load_category,
        whsc_mode,
        mode_speed_pct,
        mode_torque_pct

),

-- Ranking de eficiência entre motores por modo
ranked as (

    select
        *,
        row_number() over (
            partition by whsc_mode
            order by avg_bsfc_g_kwh asc
        )                                as bsfc_rank_in_mode,

        row_number() over (
            partition by whsc_mode
            order by avg_nox_g_kwh asc
        )                                as nox_rank_in_mode

    from performance

)

select
    engine_id,
    test_cell,
    whsc_mode,
    load_category,
    mode_speed_pct,
    mode_torque_pct,

    -- Operação
    avg_rpm,
    avg_torque_nm,
    avg_power_kw,
    max_power_kw,

    -- Eficiência
    avg_bsfc_g_kwh,
    best_bsfc_g_kwh,
    avg_fuel_flow_g_h,

    -- Emissões
    avg_nox_g_kwh,
    avg_co2_g_kwh,
    avg_pm_g_kwh,
    nox_per_kw,

    -- Temperaturas
    avg_exhaust_temp_c,
    avg_coolant_temp_c,
    avg_oil_temp_c,

    -- Turbocharger
    avg_boost_bar,
    avg_lambda,

    -- Rankings (1 = melhor no modo)
    bsfc_rank_in_mode,
    nox_rank_in_mode,

    sample_count

from ranked
order by whsc_mode, engine_id
