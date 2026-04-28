-- models/marts/emissions_compliance.sql
-- =============================================================
-- Camada Gold — mart de conformidade regulatória
-- Agrega emissões ponderadas pelo fator WHSC por teste e motor
-- Compara contra limites EPA Tier 4 Final e Euro VI
-- =============================================================

with staging as (

    select * from {{ ref('stg_dyno_measurements') }}

),

-- Emissões ponderadas pelo fator WHSC (média ponderada oficial)
weighted as (

    select
        test_id,
        engine_id,
        test_cell,
        date(timestamp)                                              as test_date,

        -- Médias ponderadas por modo WHSC (padrão regulatório)
        round(
            sum(nox_g_kwh * mode_weight_factor)
            / nullif(sum(mode_weight_factor), 0), 4
        )                                                            as weighted_nox_g_kwh,

        round(
            sum(co_g_kwh * mode_weight_factor)
            / nullif(sum(mode_weight_factor), 0), 4
        )                                                            as weighted_co_g_kwh,

        round(
            sum(co2_g_kwh * mode_weight_factor)
            / nullif(sum(mode_weight_factor), 0), 2
        )                                                            as weighted_co2_g_kwh,

        round(
            sum(hc_g_kwh * mode_weight_factor)
            / nullif(sum(mode_weight_factor), 0), 4
        )                                                            as weighted_hc_g_kwh,

        round(
            sum(pm_g_kwh * mode_weight_factor)
            / nullif(sum(mode_weight_factor), 0), 5
        )                                                            as weighted_pm_g_kwh,

        -- Contagem de amostras e falhas por teste
        count(*)                                                     as total_samples,
        countif(not epa_tier4_overall_pass)                          as epa_failures,
        countif(not euro6_overall_pass)                              as euro6_failures,
        countif(not epa_tier4_nox_pass)                             as nox_failures,
        countif(not epa_tier4_pm_pass)                              as pm_failures

    from staging
    group by
        test_id,
        engine_id,
        test_cell,
        test_date

),

-- Aplicar limites regulatórios nas emissões ponderadas
compliance as (

    select
        *,

        -- EPA Tier 4 Final limits (g/kWh)
        weighted_nox_g_kwh <= 0.40                                   as epa_nox_pass,
        weighted_co_g_kwh  <= 3.50                                   as epa_co_pass,
        weighted_hc_g_kwh  <= 0.19                                   as epa_hc_pass,
        weighted_pm_g_kwh  <= 0.02                                   as epa_pm_pass,

        -- Euro VI limits (g/kWh)
        weighted_nox_g_kwh <= 0.40                                   as euro6_nox_pass,
        weighted_co_g_kwh  <= 4.00                                   as euro6_co_pass,
        weighted_hc_g_kwh  <= 0.16                                   as euro6_hc_pass,
        weighted_pm_g_kwh  <= 0.01                                   as euro6_pm_pass,

        -- Margem em relação ao limite de NOx EPA (%)
        round(
            (0.40 - weighted_nox_g_kwh) / 0.40 * 100, 1
        )                                                            as nox_margin_pct,

        -- Failure rate
        round(epa_failures / nullif(total_samples, 0) * 100, 1)     as epa_failure_rate_pct

    from weighted

)

select
    test_id,
    engine_id,
    test_cell,
    test_date,

    -- Emissões ponderadas
    weighted_nox_g_kwh,
    weighted_co_g_kwh,
    weighted_co2_g_kwh,
    weighted_hc_g_kwh,
    weighted_pm_g_kwh,

    -- Conformidade EPA Tier 4
    epa_nox_pass,
    epa_co_pass,
    epa_hc_pass,
    epa_pm_pass,
    (epa_nox_pass and epa_co_pass
     and epa_hc_pass and epa_pm_pass)  as epa_tier4_approved,

    -- Conformidade Euro VI
    euro6_nox_pass,
    euro6_co_pass,
    euro6_hc_pass,
    euro6_pm_pass,
    (euro6_nox_pass and euro6_co_pass
     and euro6_hc_pass and euro6_pm_pass) as euro6_approved,

    -- Métricas
    nox_margin_pct,
    epa_failure_rate_pct,
    total_samples,
    epa_failures,
    euro6_failures,
    nox_failures,
    pm_failures

from compliance
order by test_date, engine_id
