# Installation

```bash
pip install pipenv
pipenv sync
```

# Running

```bash
pipenv run python src/pipeline/match.py \
  --from-host "localhost:7687" \
  --from-password "onepassword" \
  --to-host "foo.databases.neo4j.io:7687" \
  --to-password "otherpassword"
```
 
