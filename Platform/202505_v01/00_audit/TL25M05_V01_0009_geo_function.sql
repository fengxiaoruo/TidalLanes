SELECT
Default.ST_Contains(Default.ST_GeomCollection('POLYGON ((116 39,117 39,117 40,116 40,116 39))'), Default.ST_Point(116.5,39.5)) interior_should_be_true,
Default.ST_Contains(Default.ST_GeomCollection('POLYGON ((116 39,117 39,117 40,116 40,116 39))'), Default.ST_Point(116,39.5)) boundary_should_be_false;
