create table
    app.images (
        id bigint generated always as identity primary key,
        name text not null,
        is_default boolean not null
    );
