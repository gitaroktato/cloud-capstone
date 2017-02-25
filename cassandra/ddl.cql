create keyspace aviation WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
create table aviation.airport_carrier_departure (
  airport text,
  carrier text,
  dep_delay decimal,
  PRIMARY KEY(airport, dep_delay, carrier)
);