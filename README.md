# Images réutilisées sur pump.fun

Détecte les tokens qui naissent avec **l'image d'un token déjà existant**.

La comparaison se fait par **empreinte perceptuelle** (dHash 16×16, 256 bits,
canonisée miroir, confirmée par un écart quadratique sur vignette 32×32) et non
par CID IPFS : chaque copie ré-encode son image, donc le CID diffère alors que
l'image est la même.

- `scan.py` — un tour de scan, lancé toutes les 5 minutes par GitHub Actions
- `docs/index.html` — la page publiée (elle lit `docs/data.json`, même origine)
- `pump_reuse_watch.py` — les primitives (empreinte, homonymes, veille locale)

Pourquoi un scan périodique et pas un veilleur permanent dans la page : pump.fun
répond **403 à tout appel portant un en-tête `Origin`**, donc un navigateur ne
peut pas interroger son API. Le scan tourne côté GitHub Actions.

Limite assumée : une image recyclée sous un **nom différent** n'est pas
détectable — aucune recherche inverse par image n'existe publiquement.

Veille locale en temps réel (WebSocket, sans attendre le cron) :

```bash
pip install requests pillow websockets
python pump_reuse_watch.py
```
