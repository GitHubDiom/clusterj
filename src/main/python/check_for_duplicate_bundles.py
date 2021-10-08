vals = {}

with open("./input.txt", "r") as f:
   lines = f.readlines()

   for i in range(0, len(lines)):
      line = lines[i]
      splits = line.split(":")
      key = splits[0]

      if line.endswith("/"):
         next_line = lines[i+1]
         full_line = line + next_line
         i += 1
      else:
         full_line = line

      if key not in vals:
         vals[key] = full_line.strip('\n')

for key, val in vals.items():
   print(val)