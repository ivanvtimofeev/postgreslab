-- call to airport every 20-40 seconds to check timetable

while true do
  p = math.random(2,10);
  -- print(p)

  sleep(p);

  execQuery("SELECT * FROM flights WHERE departure_airport = (SELECT departure_airport FROM flights GROUP BY departure_airport ORDER BY random() limit 1) ORDER BY scheduled_departure DESC LIMIT 30");
end