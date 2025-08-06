-- Ensure at most one default image exists in the images table
-- The "at least one" constraint will be handled by application logic

-- Create a partial unique index to ensure at most one default image
create unique index images_one_default_idx on app.images (is_default) where is_default = true;