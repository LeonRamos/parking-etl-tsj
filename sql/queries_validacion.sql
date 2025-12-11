-- ============================================================
-- CONSULTAS DE VALIDACIÓN Y ANÁLISIS
-- Ejecutar en pgAdmin después de completar el DAG
-- ============================================================

-- ============================================================
-- 1. CONTEO DE REGISTROS POR CAPA (Validación básica)
-- ============================================================

-- Ejecutar esta primera para verificar que todo se cargó correctamente
SELECT 
    'BRONCE - Datos Raw' as capa, 
    COUNT(*) as total_registros,
    MIN(fecha) as fecha_mínima,
    MAX(fecha) as fecha_máxima
FROM bronce_parking_raw
UNION ALL
SELECT 
    'PLATA - Datos Limpios',
    COUNT(*),
    MIN(fecha),
    MAX(fecha)
FROM plata_parking
UNION ALL
SELECT 
    'ORO - Accesos por Hora',
    COUNT(*),
    MIN(fecha),
    MAX(fecha)
FROM oro_accesos_hora
UNION ALL
SELECT 
    'ORO - Ocupación Diaria',
    COUNT(*),
    MIN(fecha),
    MAX(fecha)
FROM oro_ocupacion_diaria
UNION ALL
SELECT 
    'ORO - Resumen Tipo Vehículo',
    COUNT(*),
    MIN(primer_acceso),
    MAX(ultimo_acceso)
FROM oro_resumen_tipo_vehiculo;

-- ============================================================
-- 2. CALIDAD DE DATOS EN PLATA
-- ============================================================

-- Porcentaje de registros válidos vs inválidos
SELECT 
    is_valid,
    COUNT(*) as cantidad,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM plata_parking), 2) as porcentaje
FROM plata_parking
GROUP BY is_valid
ORDER BY is_valid DESC;

-- Registros con errores de validación
SELECT 
    validation_errors,
    COUNT(*) as cantidad
FROM plata_parking
WHERE is_valid = FALSE
GROUP BY validation_errors
ORDER BY cantidad DESC;

-- Duplicados detectados
SELECT 
    COUNT(*) as total_duplicados
FROM plata_parking
WHERE duplicate_marker = 'DUPLICADO';

-- ============================================================
-- 3. ANÁLISIS DE OCUPACIÓN - HORAS CRÍTICAS
-- ============================================================

-- Top 20 horas con mayor ocupación
SELECT 
    ah.fecha,
    ah.hora,
    ah.tipo_vehiculo,
    ah.total_accesos,
    ah.capacidad_sobrepasada_count,
    CASE 
        WHEN ah.capacidad_sobrepasada_count > 0 THEN '🔴 CRÍTICO - SOBREPASADO'
        WHEN ah.total_accesos > 50 THEN '🟠 ALTO'
        WHEN ah.total_accesos > 30 THEN '🟡 MEDIO'
        ELSE '🟢 BAJO'
    END as nivel_alerta
FROM oro_accesos_hora ah
ORDER BY ah.capacidad_sobrepasada_count DESC, ah.total_accesos DESC
LIMIT 20;

-- Horas pico (busiest hours) sin importar día
SELECT 
    ah.hora,
    ah.tipo_vehiculo,
    COUNT(*) as días_con_datos,
    ROUND(AVG(ah.total_accesos), 2) as promedio_accesos,
    MAX(ah.total_accesos) as máximo_accesos,
    SUM(ah.capacidad_sobrepasada_count) as eventos_críticos
FROM oro_accesos_hora ah
GROUP BY ah.hora, ah.tipo_vehiculo
ORDER BY eventos_críticos DESC, promedio_accesos DESC
LIMIT 15;

-- ============================================================
-- 4. ANÁLISIS POR TIPO DE VEHÍCULO
-- ============================================================

-- Resumen completo por tipo de vehículo
SELECT
    tipo_vehiculo,
    total_accesos,
    total_accesos_validos,
    total_accesos_invalidos,
    porcentaje_validez,
    primer_acceso,
    ultimo_acceso,
    días_con_acceso,
    ROUND(total_accesos::numeric / NULLIF(días_con_acceso, 0), 2) as promedio_por_día,
    accesos_con_engomado,
    accesos_con_bitacora,
    accesos_descanso_oficial,
    accesos_vacacional,
    capacidad_sobrepasada_events,
    ROUND(100.0 * capacidad_sobrepasada_events / NULLIF(total_accesos, 0), 2) as porcentaje_crítico
FROM oro_resumen_tipo_vehiculo
ORDER BY total_accesos DESC;

-- Comparativa: Vehículos que más sobrepasan capacidad
SELECT
    tipo_vehiculo,
    capacidad_sobrepasada_events,
    total_accesos,
    ROUND(100.0 * capacidad_sobrepasada_events / NULLIF(total_accesos, 0), 2) as porcentaje_crítico,
    ROUND(100.0 * accesos_con_engomado / NULLIF(total_accesos, 0), 2) as porcentaje_engomado
FROM oro_resumen_tipo_vehiculo
WHERE capacidad_sobrepasada_events > 0
ORDER BY capacidad_sobrepasada_events DESC;

-- ============================================================
-- 5. PATRONES TEMPORALES
-- ============================================================

-- Ocupación por día de la semana
SELECT 
    od.día_semana,
    od.tipo_vehiculo,
    COUNT(*) as días_únicos,
    ROUND(AVG(od.total_accesos), 2) as promedio_accesos,
    SUM(od.total_accesos) as total_accesos,
    SUM(od.capacidad_sobrepasada_events) as eventos_críticos
FROM oro_ocupacion_diaria od
WHERE od.día_semana IS NOT NULL
GROUP BY od.día_semana, od.tipo_vehiculo
ORDER BY 
    CASE 
        WHEN od.día_semana = 'Monday' THEN 1
        WHEN od.día_semana = 'Tuesday' THEN 2
        WHEN od.día_semana = 'Wednesday' THEN 3
        WHEN od.día_semana = 'Thursday' THEN 4
        WHEN od.día_semana = 'Friday' THEN 5
        WHEN od.día_semana = 'Saturday' THEN 6
        WHEN od.día_semana = 'Sunday' THEN 7
    END,
    total_accesos DESC;

-- Comparación: Descansos oficiales vs días normales
SELECT 
    'Descanso Oficial' as tipo_día,
    COUNT(*) as registros,
    ROUND(AVG(total_accesos), 2) as promedio_accesos,
    SUM(total_accesos) as total_accesos,
    SUM(capacidad_sobrepasada_events) as eventos_críticos
FROM oro_ocupacion_diaria
WHERE descanso_oficial = TRUE
UNION ALL
SELECT 
    'Día Normal',
    COUNT(*),
    ROUND(AVG(total_accesos), 2),
    SUM(total_accesos),
    SUM(capacidad_sobrepasada_events)
FROM oro_ocupacion_diaria
WHERE descanso_oficial = FALSE;

-- Comparación: Períodos vacacionales
SELECT 
    CASE WHEN vacacional = TRUE THEN 'Período Vacacional' ELSE 'Período Lectivo' END as período,
    COUNT(*) as registros,
    ROUND(AVG(total_accesos), 2) as promedio_accesos,
    SUM(total_accesos) as total_accesos,
    SUM(capacidad_sobrepasada_events) as eventos_críticos
FROM oro_ocupacion_diaria
GROUP BY vacacional;

-- ============================================================
-- 6. TENDENCIAS ANUALES
-- ============================================================

-- Comparativa año a año (si hay múltiples años)
SELECT 
    od.año,
    od.tipo_vehiculo,
    COUNT(DISTINCT od.fecha) as días_únicos,
    SUM(od.total_accesos) as total_accesos,
    ROUND(AVG(od.total_accesos), 2) as promedio_por_día,
    SUM(od.capacidad_sobrepasada_events) as eventos_críticos
FROM oro_ocupacion_diaria od
GROUP BY od.año, od.tipo_vehiculo
ORDER BY od.año DESC, total_accesos DESC;

-- ============================================================
-- 7. ANÁLISIS DE ENGOMADO Y BITÁCORA
-- ============================================================

-- Relación entre engomado/bitácora y validez
SELECT 
    CASE WHEN engomado THEN 'Con Engomado' ELSE 'Sin Engomado' END as estado_engomado,
    CASE WHEN bitacora THEN 'Con Bitácora' ELSE 'Sin Bitácora' END as estado_bitácora,
    COUNT(*) as cantidad,
    SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as válidos,
    ROUND(100.0 * SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) / COUNT(*), 2) as porcentaje_válidos
FROM plata_parking
GROUP BY engomado, bitacora
ORDER BY cantidad DESC;

-- ============================================================
-- 8. ANÁLISIS POR BARRA DE ACCESO
-- ============================================================

-- Distribución de tráfico por barra de acceso
SELECT 
    barra_acceso,
    COUNT(*) as total_accesos,
    COUNT(DISTINCT DATE(timestamp)) as días_operativo,
    ROUND(AVG(EXTRACT(HOUR FROM timestamp))::numeric, 1) as hora_promedio,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM plata_parking WHERE barra_acceso IS NOT NULL), 2) as porcentaje
FROM plata_parking
WHERE barra_acceso IS NOT NULL AND is_valid = TRUE
GROUP BY barra_acceso
ORDER BY total_accesos DESC;

-- ============================================================
-- 9. TOP 10 PLACAS MÁS FRECUENTES
-- ============================================================

SELECT 
    placa,
    COUNT(*) as total_accesos,
    COUNT(DISTINCT DATE(timestamp)) as días_únicos,
    MIN(timestamp) as primer_acceso,
    MAX(timestamp) as último_acceso,
    (SELECT tipo_vehiculo FROM plata_parking p2 WHERE p2.placa = p1.placa LIMIT 1) as tipo_vehículo
FROM plata_parking p1
WHERE is_valid = TRUE
GROUP BY placa
ORDER BY total_accesos DESC
LIMIT 10;

-- ============================================================
-- 10. IDENTIFICACIÓN DE ANOMALÍAS
-- ============================================================

-- Registros con anomalías detectadas
SELECT 
    timestamp,
    placa,
    tipo_vehiculo,
    validation_errors,
    duplicate_marker
FROM plata_parking
WHERE is_valid = FALSE OR duplicate_marker = 'DUPLICADO'
ORDER BY timestamp DESC
LIMIT 20;

-- Placas con accesos en horarios inusuales (después de medianoche)
SELECT 
    DATE(timestamp) as fecha,
    EXTRACT(HOUR FROM timestamp)::INT as hora,
    placa,
    tipo_vehiculo,
    COUNT(*) as cantidad
FROM plata_parking
WHERE EXTRACT(HOUR FROM timestamp) NOT BETWEEN 5 AND 22
  AND is_valid = TRUE
GROUP BY DATE(timestamp), EXTRACT(HOUR FROM timestamp), placa, tipo_vehiculo
ORDER BY fecha DESC, hora
LIMIT 20;

-- ============================================================
-- 11. MÉTRICAS DE DESEMPEÑO DEL PIPELINE
-- ============================================================

-- Log de ejecuciones (si se registra en tabla de auditoría)
SELECT 
    dag_id,
    task_id,
    execution_date,
    task_duration_seconds,
    status,
    records_processed,
    records_inserted,
    CASE 
        WHEN records_processed > 0 
        THEN ROUND(100.0 * records_inserted / records_processed, 2)
        ELSE NULL
    END as tasa_éxito_porcentaje
FROM oro_execution_log
ORDER BY execution_date DESC
LIMIT 20;

-- ============================================================
-- 12. VISTAS CONSOLIDADAS (si existen)
-- ============================================================

-- Ver resumen diario consolidado
SELECT 
    fecha,
    año,
    día_semana,
    tipo_vehiculo,
    total_accesos,
    capacidad_sobrepasada_events,
    pico_hora,
    pico_valor,
    CASE 
        WHEN descanso_oficial THEN '🔴 Descanso Oficial'
        WHEN vacacional THEN '🟠 Período Vacacional'
        ELSE '🟢 Normal'
    END as tipo_día
FROM v_resumen_diario_consolidado
ORDER BY fecha DESC
LIMIT 30;

-- Horarios críticos (donde se sobrepasó capacidad)
SELECT 
    fecha,
    hora,
    tipo_vehiculo,
    total_accesos,
    capacidad_sobrepasada_count,
    nivel_ocupacion
FROM v_horarios_criticos
ORDER BY fecha DESC, hora DESC
LIMIT 20;

-- ============================================================
-- 13. EXPORTACIÓN DE DATOS PARA ANÁLISIS EXTERNO
-- ============================================================

-- Resumen ejecutivo diario (para reportes)
SELECT 
    fecha,
    COUNT(DISTINCT tipo_vehiculo) as tipos_vehículos,
    SUM(total_accesos) as total_accesos,
    MAX(pico_valor) as máximo_por_hora,
    ROUND(AVG(promedio_accesos_hora), 2) as promedio_por_hora,
    SUM(CASE WHEN capacidad_sobrepasada_events > 0 THEN 1 ELSE 0 END) as tipos_con_capacidad_sobrepasada
FROM oro_ocupacion_diaria
GROUP BY fecha
ORDER BY fecha DESC
LIMIT 30;

-- ============================================================
-- 14. CONSULTAS PARA TOMA DE DECISIONES
-- ============================================================

-- Recomendación 1: ¿Qué tipo de vehículo está causando problemas?
SELECT 
    tipo_vehiculo,
    capacidad_sobrepasada_events,
    ROUND(100.0 * capacidad_sobrepasada_events / total_accesos, 2) as porcentaje_eventos_críticos
FROM oro_resumen_tipo_vehiculo
WHERE capacidad_sobrepasada_events > 0
ORDER BY capacidad_sobrepasada_events DESC;

-- Recomendación 2: ¿Cuáles son los horarios más críticos?
SELECT 
    ah.hora,
    COUNT(*) as días_críticos,
    ROUND(AVG(ah.capacidad_sobrepasada_count), 2) as promedio_eventos_críticos,
    STRING_AGG(DISTINCT tipo_vehiculo, ', ') as tipos_afectados
FROM oro_accesos_hora ah
WHERE ah.capacidad_sobrepasada_count > 0
GROUP BY ah.hora
ORDER BY promedio_eventos_críticos DESC
LIMIT 10;

-- Recomendación 3: Necesidad de ampliación por período
SELECT 
    CASE 
        WHEN vacacional THEN 'Período Vacacional'
        WHEN descanso_oficial THEN 'Descanso Oficial'
        ELSE 'Período Lectivo Normal'
    END as período,
    SUM(total_accesos) as total_accesos,
    SUM(capacidad_sobrepasada_events) as eventos_críticos,
    ROUND(100.0 * SUM(capacidad_sobrepasada_events) / SUM(total_accesos), 2) as porcentaje_crítico
FROM oro_ocupacion_diaria
GROUP BY descanso_oficial, vacacional
ORDER BY eventos_críticos DESC;

-- ============================================================
-- FIN DE CONSULTAS DE VALIDACIÓN
-- ============================================================
