-- Support NATIVE_FFM (.so / .dylib / .dll via JDK 25 FFM & GraalVM --shared) plugin type (#36)

ALTER TABLE plugins
    DROP CONSTRAINT IF EXISTS plugins_type_check;

ALTER TABLE plugins
    ADD CONSTRAINT plugins_type_check
    CHECK (type IN ('BUILTIN', 'JAVA_SPI', 'NATIVE_FFM', 'JS', 'WEBHOOK'));
