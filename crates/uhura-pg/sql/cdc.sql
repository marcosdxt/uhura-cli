-- Função genérica de captura CDC (trigger-based). Ver SPEC.md §13.2.
-- Args do trigger: TG_ARGV[0]=contrato (domínio), TG_ARGV[1]=coluna de id.
-- Escreve um envelope CloudEvents no uhura_outbox a cada mudança de linha.
CREATE OR REPLACE FUNCTION uhura_cdc_capture() RETURNS trigger AS $$
DECLARE
  v_contract text := TG_ARGV[0];
  v_id_col   text := TG_ARGV[1];
  v_event    text;
  v_rec      jsonb;
  v_id       text;
BEGIN
  IF TG_OP = 'INSERT' THEN
    v_event := 'inserted'; v_rec := to_jsonb(NEW);
  ELSIF TG_OP = 'UPDATE' THEN
    v_event := 'updated';  v_rec := to_jsonb(NEW);
  ELSE
    v_event := 'removed';  v_rec := to_jsonb(OLD);
  END IF;

  v_id := v_rec ->> v_id_col;

  INSERT INTO uhura_outbox (domain, event, partitionkey, envelope)
  VALUES (
    v_contract,
    v_event,
    v_id,
    jsonb_build_object(
      'id', gen_random_uuid()::text,
      'source', 'pg:' || TG_TABLE_NAME,
      'specversion', '1.0',
      'type', v_contract || '.' || v_event,
      'subject', v_id,
      'time', to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
      'datacontenttype', 'application/json',
      'partitionkey', v_id,
      'facttype', 'SNAPSHOT',
      'data', v_rec
    )
  );
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
