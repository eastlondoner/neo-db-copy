# Installation

```bash
pip install pipenv
pipenv sync
```

# Running Database Match

This checks that two neo4j databases are equal

```bash
pipenv run python src/pipeline/match.py \
  --from-host "localhost:7687" \
  --from-password "onepassword" \
  --to-host "foo.databases.neo4j.io:7687" \
  --to-password "otherpassword" \
  --relationship-key "uid" \
  --node-key "name"
```
 
