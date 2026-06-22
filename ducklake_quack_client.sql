CREATE SECRET (
    TYPE quack,
    TOKEN 'super_secret',
    SCOPE 'quack:localhost'
);

ATTACH 'quack:localhost' AS warehouse (TYPE quack);

CALL warehouse.query('USE warehouse.hybench_sf1x');
