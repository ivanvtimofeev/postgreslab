-- call to airport every 20-40 seconds to check timetable

while true do
  p = math.random(2,10);
  -- print(p)

  sleep(p);

  execQuery("SELECT * FROM timetable");
end