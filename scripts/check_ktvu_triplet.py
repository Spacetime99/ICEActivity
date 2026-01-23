import json
items = json.load(open("frontend/public/data/triplets_3d.json", "r", encoding="utf-8"))
match = [i for i in items if "fmc-6tqj05v9b8no0gfh" in i.get("story_id", "")]
print(match[0] if match else "no match")
