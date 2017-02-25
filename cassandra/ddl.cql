create keyspace aviation WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
create table aviation.airport_carrier_departure (
  airport text,
  carrier text,
  dep_delay decimal,
  PRIMARY KEY(airport, dep_delay, carrier)
);
create table aviation.airport_airport_departure (
  airport text,
  airport_to text,
  dep_delay decimal,
  PRIMARY KEY(airport, dep_delay, airport_to)
);
create table aviation.airport_airport_arrival (
  airport text,
  airport_to text,
  arr_delay decimal,
  PRIMARY KEY(airport, airport_to)
);