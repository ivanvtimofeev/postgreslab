-- call to airport every 1-10 milli seconds to check timetable

while true do
  p = math.random(1,10);
  sleep(p);

  execQuery("SELECT flight_id, flight_no, departure_airport, arrival_airport FROM flights WHERE departure_airport = (SELECT departure_airport FROM flights GROUP BY departure_airport ORDER BY random() limit 1) ORDER BY scheduled_departure DESC LIMIT 30;");
end