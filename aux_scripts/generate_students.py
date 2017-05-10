import random, json, string

count = 2**24
with open("students.json", "w") as fp:
    for i  in range(count):
        name = random.choice(string.uppercase)
        for j in range(random.randint(1,7)):
            name += random.choice("aeiou") + random.choice(string.lowercase)
        height = str(random.choice([5,5,5,6])) + "'" + str(random.randint(0,11)) + "\""
        entry = {"name": name, 
                 "gender":random.choice(["Male", "Female"]),
                 "weight":random.randint(100,250), 
                 "height":height,
                 "age":random.randint(18,65),
                 "grade":random.choice(['A','A-','B+','B','B-','C','D','F']),
                 "points":random.randint(0,10000),
                 "department":random.choice(["ENG", "COS", "CHM", "PHY", "ELE"])
                }
        json.dump(entry,fp)
        fp.write("\n")
