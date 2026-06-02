-- Function to atomically create (if needed) and set the default image by name.
-- Usage: select app.update_default_image('supabase/etl-replicator:1.2.3');

create or replace function app.update_default_image(new_image_name text)
returns void
language plpgsql
as $$
declare
  new_image_id bigint;
begin
  -- Serialize concurrent default switches using an advisory transaction lock.
  perform pg_advisory_xact_lock(('app.images'::regclass::oid)::bigint);

  -- Insert if missing; don't set default yet to respect the partial unique index.
  insert into app.images(name, is_default)
  values (new_image_name, false)
  on conflict (name) do nothing
  returning id into new_image_id;

  -- If the image already existed, fetch its id.
  if new_image_id is null then
    select id into new_image_id from app.images where name = new_image_name;
  end if;

  -- Unset any existing default that is not the requested image.
  update app.images
     set is_default = false, updated_at = now()
   where is_default = true
     and id <> new_image_id;

  -- Set the requested image as default.
  update app.images
     set is_default = true, updated_at = now()
   where id = new_image_id;
end
$$;