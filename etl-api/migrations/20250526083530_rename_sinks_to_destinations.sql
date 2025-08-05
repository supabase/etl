-- alter table doesn't allow schema qualification in the new name
-- so we use destinations here instead of app.destinations
alter table app.sinks rename to destinations;