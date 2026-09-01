#!/usr/bin/env python3
"""Veille pump.fun : alerte quand un token recycle un ANCIEN token qui avait deja couru.

    python scripts/pump_reuse_watch.py                   # veille live
    python scripts/pump_reuse_watch.py --check MINT       # rapport sur un mint
    python scripts/pump_reuse_watch.py --replay SYMBOLE   # rejoue la regle sur un cluster passe
    python scripts/pump_reuse_watch.py --selftest

Le signal n'est pas "un nom deja utilise" (des fermes en sortent 60 par heure qui
plafonnent a 3K). Le signal est : le ticker a DEJA couru il y a des mois, et le
cluster du jour est encore peu encombre -- donc on n'arrive pas 19e.

Deux pieges tranches par la mesure :
  - le CID IPFS ne sert a rien pour comparer les images (chaque clone re-encode,
    donc CID different pour la meme image) => hash perceptuel dHash 64 bits ;
  - la recherche pump.fun triee par date noie l'original ancien derriere les
    clones du jour => on interroge AUSSI sort=ath_market_cap, qui remonte les
    meilleurs homonymes de tous les temps.
"""
import argparse, asyncio, base64, io, json, os, subprocess, threading, time
from urllib.parse import urlparse
import requests
from PIL import Image

PUMP = "https://frontend-api-v3.pump.fun"
WSURL = "wss://pumpportal.fun/api/data"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
HASH_CACHE = os.path.join(DATA, "pump_img_prints_v2.json")   # v1 = dH8 seul, incompatible
ALERT_LOG = os.path.join(DATA, "pump_reuse_alerts.jsonl")

# Regle par defaut : ALERTE DES QU'UNE IMAGE EST REUTILISEE. Rien d'autre.
# Les seuils ci-dessous sont des filtres OPTIONNELS, neutres par defaut.
RUNNER = 0             # ATH minimum du token source (0 = on ne filtre pas)
MIN_AGE_DAYS = 0       # age minimum du token source en jours (0 = on ne filtre pas)
MAX_CROWD_24H = 10**9  # homonymes max nes dans les 24h (enorme = on ne filtre pas)
DHASH_SIZE = 16        # 256 bits. dH8 donnait un faux positif demontre (UKR/POL)
IMG_MAX_DIST = 48      # hamming <= 48 sur 256 bits
RMSE_MAX = 0.5         # 2e etage de confirmation sur vignette 32x32 normalisee
FLAT_STD_MIN = 5.0     # image trop plate (fond uni) => verdict image impossible
PAGE = 70              # /coins ecrete limit a 70 EN SILENCE (100/200/500 -> 70)
MAX_PAGES = 9          # au-dela le jeu de mints se repete a l'identique
API_429_PAUSE = 180.0  # /coins/{mint} soutenu -> IP bloquee ~177s
API_MIN_GAP = 0.5      # throttle pump.fun (s entre 2 appels)
DAY = 86400.0

# La recherche plafonne a ~70 resultats et le tri decide LESQUELS on recoit :
# il faut donc les deux passes pour voir a la fois l'histoire et l'actualite.
SEARCH_SORTS = (("ath_market_cap", "DESC"), ("created_timestamp", "DESC"))

# Passerelles de metadonnees normales. Tout autre host = lanceur de masse
# (j7tracker.io, uxento.io, pumper.ink, rapidlaunch.io : les outils de
# copier-coller de token). Signal gratuit, deja present dans le message WS.
NORMAL_META_HOSTS = {
    "ipfs.io", "cf-ipfs.com", "pump.mypinata.cloud", "gateway.pinata.cloud",
    "gateway.irys.xyz", "arweave.net", "storage.googleapis.com", "nftstorage.link",
    "dweb.link", "w3s.link", "ipfs.dweb.link",
}

S = requests.Session()
S.headers["accept"] = "application/json"
_img_prints = json.load(open(HASH_CACHE)) if os.path.exists(HASH_CACHE) else {}
_clones = {}       # symbol.lower() -> (ts, [coins])
_last_api = 0.0
_api_pause_until = 0.0
_api_lock = threading.Lock()


# ---------- hash perceptuel ----------
def _dhash_px(im, size):
    px = im.convert("L").resize((size + 1, size), Image.LANCZOS).tobytes()
    bits = 0
    for r in range(size):
        row = px[r * (size + 1):(r + 1) * (size + 1)]
        for c in range(size):
            bits = (bits << 1) | int(row[c] < row[c + 1])
    return bits


def dhash(img_bytes, size=None):
    """dHash canonise : min(hash, hash du miroir). Un clone qui retourne l'image
    reste detecte, et la canonisation ne coute rien."""
    from PIL import ImageOps
    size = size or DHASH_SIZE
    im = Image.open(io.BytesIO(img_bytes))
    return min(_dhash_px(im, size), _dhash_px(ImageOps.mirror(im), size))


def thumb32(img_bytes):
    return Image.open(io.BytesIO(img_bytes)).convert("L").resize((32, 32), Image.LANCZOS).tobytes()


def _norm32(t):
    n = len(t)
    m = sum(t) / n
    sd = (sum((v - m) ** 2 for v in t) / n) ** 0.5
    return sd, ([(v - m) / sd for v in t] if sd > 0 else None)


def rmse32(ta, tb):
    """Ecart quadratique sur vignettes normalisees. None si une image est trop
    plate (fond uni) : dans ce cas le hash seul produit des faux positifs."""
    sda, na = _norm32(ta)
    sdb, nb = _norm32(tb)
    if na is None or nb is None or sda < FLAT_STD_MIN or sdb < FLAT_STD_MIN:
        return None
    return (sum((x - y) ** 2 for x, y in zip(na, nb)) / len(na)) ** 0.5


def _mirror32(t):
    return b"".join(t[r * 32:(r + 1) * 32][::-1] for r in range(32))


def same_image(fa, fb):
    """Deux etages : hamming dH16 puis confirmation rmse. dH16 seul ne suffit pas.

    Le hash est canonise miroir, donc la vignette doit l'etre aussi : on prend le
    meilleur des deux orientations, sinon un clone retourne passe l'etage 1 et
    echoue l'etage 2 pour rien.
    """
    if not fa or not fb:
        return False, None
    d = hamming(fa[0], fb[0])
    if d > IMG_MAX_DIST:
        return False, d
    ecarts = [x for x in (rmse32(fa[1], fb[1]), rmse32(fa[1], _mirror32(fb[1])))
              if x is not None]
    return (bool(ecarts) and min(ecarts) < RMSE_MAX), d


def hamming(a, b):
    return bin(a ^ b).count("1")


# ipfs.io repond bien (le 403 rapporte venait d'un User-Agent navigateur) et
# c'est la passerelle la plus rapide ET l'hote dominant. Le vrai mode de panne
# est le timeout sous concurrence -> repli sur d'autres passerelles.
GATEWAY_FALLBACK = ("https://pump.mypinata.cloud/ipfs/", "https://dweb.link/ipfs/")


def _fetch_image(url):
    urls = [url]
    if "/ipfs/" in url:
        cid = url.split("/ipfs/", 1)[1]
        urls += [g + cid for g in GATEWAY_FALLBACK if not url.startswith(g)]
    for u in urls:
        try:
            r = S.get(u, timeout=8)
            if r.ok and 0 < len(r.content) < 8_000_000:
                return r.content
        except Exception:
            continue
    return None


def img_print(url):
    """Empreinte (dH16 canonise, vignette 32x32) d'une image distante. Cachee."""
    if not url:
        return None
    if url in _img_prints:
        v = _img_prints[url]
        return (v[0], base64.b64decode(v[1])) if v else None
    out = None
    b = _fetch_image(url)
    if b:
        try:
            out = (dhash(b), thumb32(b))
        except Exception:
            out = None
    _img_prints[url] = [out[0], base64.b64encode(out[1]).decode()] if out else None
    return out


def save_cache():
    os.makedirs(DATA, exist_ok=True)
    tmp = HASH_CACHE + ".tmp"
    json.dump(_img_prints, open(tmp, "w"))
    os.replace(tmp, HASH_CACHE)


# ---------- pump.fun ----------
def api(path, **params):
    """Appel pump.fun throttle. Un 429 bloque l'IP ~177s : on attend et on retente."""
    global _last_api, _api_pause_until
    for essai in (1, 2):
        with _api_lock:
            wait = max(API_MIN_GAP - (time.time() - _last_api),
                       _api_pause_until - time.time())
            if wait > 0:
                time.sleep(wait)
            _last_api = time.time()
        r = S.get(PUMP + path, params=params, timeout=20)
        if r.status_code == 429:
            with _api_lock:
                _api_pause_until = time.time() + API_429_PAUSE
            if essai == 1:
                continue
        r.raise_for_status()
        return r.json()


def _coins(payload):
    if isinstance(payload, dict):
        return payload.get("coins") or payload.get("data") or []
    return payload or []


ATH_MAX_SANE = 50_000_000   # au-dela c'est une donnee corrompue (vu $587M dans un rejeu)


def ath(c):
    v = c.get("ath_market_cap") or 0
    return 0 if v > ATH_MAX_SANE else v


def age_days(c, now=None):
    ts = (c.get("created_timestamp") or 0) / 1000
    return ((now or time.time()) - ts) / DAY if ts else 0.0


def _search(symbol, sort, order, ttl=600):
    """Une passe de recherche pump.fun, filtree sur l'homonymie exacte. Cachee."""
    key = (symbol or "").strip().lower()
    if not key:
        return [], 0
    ck = (key, sort)
    hit = _clones.get(ck)
    if hit and time.time() - hit[0] < ttl:
        return hit[1], hit[2]
    exact, fuzzy, vus = {}, 0, set()
    # Sans offset on ne recuperait qu'UNE page de 70 lignes, soit ~19% du cluster
    # de BLINDAPE (327 exacts au total) -- et on ratait 6 des 10 meilleurs
    # jumeaux par ATH, ceux qui portent justement le signal.
    for page in range(MAX_PAGES):
        try:
            d = _coins(api("/coins", searchTerm=symbol, limit=PAGE, includeNsfw="true",
                           sort=sort, order=order, offset=page * PAGE))
        except Exception:
            break
        if not d:
            break
        sig = frozenset(c.get("mint") for c in d)
        if sig in vus:        # le jeu sature puis se repete a l'identique
            break
        vus.add(sig)
        for c in d:
            m = c.get("mint")
            if not m:
                continue
            names = (str(c.get("symbol", "")).strip().lower(),
                     str(c.get("name", "")).strip().lower())
            # la recherche est floue ("puter" ressort sur BLINDAPE) : l'exact
            # porte l'historique, le flou ne mesure que la bousculade
            if key in names:
                exact[m] = c
            else:
                fuzzy += 1
        if len(d) < PAGE:
            break
    _clones[ck] = (time.time(), list(exact.values()), fuzzy)
    return _clones[ck][1], fuzzy


def clones_of(symbol, full=False):
    """Homonymes exacts. full=False : 1 requete (l'historique, tri par ATH), qui
    suffit a trancher 95% des cas. full=True : + l'actualite (tri par date), qui
    ne sert qu'a mesurer la bousculade d'un vrai candidat.

    La recherche plafonne a ~70 resultats et le tri decide LESQUELS on recoit :
    d'ou les deux passes quand on a besoin des deux bouts.
    """
    hist, nf = _search(symbol, "ath_market_cap", "DESC")
    if not full:
        return hist, nf
    rec, nf2 = _search(symbol, "created_timestamp", "DESC")
    merged = {c["mint"]: c for c in hist}
    merged.update({c["mint"]: c for c in rec})
    return list(merged.values()), max(nf, nf2)


def social_keys(c):
    """URLs sociales normalisees d'un coin (vide si aucune)."""
    out = set()
    for k in ("website", "twitter", "telegram"):
        v = str(c.get(k) or "").strip().lower().rstrip("/")
        if v.startswith("http"):
            out.add(v)
    return out


def launcher_of(metadata_uri):
    h = urlparse(str(metadata_uri or "")).netloc.lower()
    return None if (not h or h in NORMAL_META_HOSTS) else h


# ---------- verdict ----------
def report(mint, symbol=None, image_uri=None, creator=None, metadata_uri=None,
           now=None, probe=6, coin=None):
    """Evalue un mint. now= rejoue une situation passee, coin= evite un appel API."""
    if coin is not None:
        symbol = symbol or coin.get("symbol") or coin.get("name")
        creator = creator or coin.get("creator")
        image_uri = image_uri or coin.get("image_uri")
        metadata_uri = metadata_uri or coin.get("metadata_uri")
    elif symbol is None or creator is None:
        coin = api(f"/coins/{mint}")
        symbol = symbol or coin.get("symbol") or coin.get("name")
        creator = creator or coin.get("creator")
        image_uri = image_uri or coin.get("image_uri")
        metadata_uri = metadata_uri or coin.get("metadata_uri")
    now = now or time.time()

    def _live(lst):
        return [c for c in lst if c.get("mint") != mint
                and (c.get("created_timestamp") or 0) / 1000 <= now]

    twins, n_fuzzy = clones_of(symbol)          # 1 requete : l'historique du ticker
    twins = _live(twins)
    old_runners = [c for c in twins if ath(c) >= RUNNER and age_days(c, now) >= MIN_AGE_DAYS]
    best_old = max(old_runners, key=ath) if old_runners else None

    n_img, img_best, shared_social = 0, None, []
    n_24h = sum(1 for c in twins if age_days(c, now) <= 1.0)
    if twins:
        # On compare l'image a celle des homonymes -- TOUJOURS, sans condition
        # d'ATH : la question posee est "cette image a-t-elle deja servi ?".
        probes, seen = [], set()
        for c in ([best_old] if best_old else []) + sorted(twins, key=lambda c: -ath(c)):
            if c.get("mint") not in seen:
                seen.add(c["mint"]); probes.append(c)
            if len(probes) >= probe:
                break
        # L'image du candidat. Le metadata json (IPFS) ne consomme pas le budget
        # de rate limit pump.fun : on le prefere, /coins/{mint} en repli.
        if not image_uri and metadata_uri:
            try:
                image_uri = (S.get(metadata_uri, timeout=8).json() or {}).get("image")
            except Exception:
                pass
        if not image_uri:
            try:
                coin = coin or api(f"/coins/{mint}")
                image_uri = coin.get("image_uri")
            except Exception:
                pass

        fa = img_print(image_uri)
        if fa is not None:
            for c in probes:
                fb = img_print(c.get("image_uri"))
                if fb is None:
                    continue
                ok, dd = same_image(fa, fb)
                if dd is not None and (img_best is None or dd < img_best[0]):
                    img_best = (dd, c)
                if ok:
                    n_img += 1

        mine = social_keys(coin) if coin else set()
        theirs = set().union(*(social_keys(c) for c in twins)) if twins else set()
        shared_social = sorted(mine & theirs)

    early = n_24h <= MAX_CROWD_24H
    return {
        "mint": mint, "symbol": symbol, "creator": creator,
        "launcher": launcher_of(metadata_uri),
        "n_twins": len(twins), "n_fuzzy": n_fuzzy, "n_24h": n_24h, "early": early,
        "best_old_mint": best_old.get("mint") if best_old else None,
        "best_old_ath": ath(best_old) if best_old else 0,
        "best_old_age_days": round(age_days(best_old, now), 1) if best_old else None,
        "ceiling": ath(best_old) if best_old else 0,
        "n_same_image": n_img,
        # details de la source, pour pouvoir l'afficher sans nouvelle requete
        "img_twin_symbol": (img_best[1].get("symbol") or img_best[1].get("name")) if img_best else None,
        "img_twin_cree": ((img_best[1].get("created_timestamp") or 0) / 1000) if img_best else None,
        "img_twin_ath": ath(img_best[1]) if img_best else 0,
        "img_twin_image": img_best[1].get("image_uri") if img_best else None,
        "img_dist": img_best[0] if img_best else None,
        "img_twin": img_best[1].get("mint") if img_best else None,
        "shared_social": shared_social[:1],
        # ALERTE = image reutilisee. Les autres conditions sont neutres par defaut.
        "alert": n_img > 0 and early
                 and (not RUNNER or (img_best and ath(img_best[1]) >= RUNNER))
                 and (not MIN_AGE_DAYS or (img_best and age_days(img_best[1], now) >= MIN_AGE_DAYS)),
    }


def line(r):
    tag = "ALERTE" if r["alert"] else "  ."
    hist = (f"a couru ${r['best_old_ath']:,.0f} il y a {r['best_old_age_days']:.0f}j"
            if r["best_old_mint"] else "aucun passe >= seuil")
    img = ("image REUTILISEE" if r["n_same_image"]
           else f"image differente (d={r['img_dist']})" if r["img_dist"] is not None
           else "image non comparee")
    return (f"{tag} {str(r['symbol'])[:14]:<14} {hist:<32} | {r['n_24h']:>2} en 24h"
            f"{' TOT' if r['early'] else ' tard'} | {img}"
            f"{' | social recycle' if r['shared_social'] else ''}"
            f"{' | lanceur:' + r['launcher'] if r['launcher'] else ''} | {r['mint']}")


def explain(r):
    """Le meme verdict, en phrases -- c'est ce qu'on lit a 3h du matin."""
    L = [f"  {r['symbol']}  ({r['mint']})"]
    if r["best_old_mint"]:
        L.append(f"  Ce ticker a deja couru : ${r['best_old_ath']:,.0f} de market cap, "
                 f"il y a {r['best_old_age_days']:.0f} jours. C'est le plafond a viser.")
    else:
        L.append("  Ce ticker n'a aucun passe au-dessus du seuil : rien ne prouve qu'il peut courir.")
    L.append(f"  {r['n_twins']} homonymes exacts en tout, dont {r['n_24h']} nes dans les 24h"
             f" (+{r['n_fuzzy']} variantes approchantes).")
    L.append("  => Tu es TOT dans le cluster." if r["early"]
             else "  => La bousculade a deja commence : arriver maintenant, c'est tirer au sort.")
    if r["n_same_image"]:
        L.append(f"  L'image est celle de {r['img_twin']} (distance {r['img_dist']}).")
    if r["shared_social"]:
        L.append(f"  Meme reseau social qu'un homonyme : {r['shared_social'][0]}")
    if r["launcher"]:
        L.append(f"  Lance par un outil de masse ({r['launcher']}), pas a la main.")
    return "\n".join(L)


def notify(title, msg):
    try:
        subprocess.run(["osascript", "-e",
                        f"display notification {json.dumps(msg)} with title {json.dumps(title)} "
                        f"sound name \"Ping\""], timeout=5, capture_output=True)
    except Exception:
        pass


def emit(r, verbose=False):
    print(line(r), flush=True)
    if not r["alert"]:
        return
    print(explain(r), flush=True)
    notify(f"{r['symbol']} — ancien runner recycle",
           f"a fait ${r['best_old_ath']:,.0f} il y a {r['best_old_age_days']:.0f}j "
           f"| {r['n_24h']} clones en 24h")
    os.makedirs(DATA, exist_ok=True)
    with open(ALERT_LOG, "a") as f:
        f.write(json.dumps({**r, "ts": time.time()}) + "\n")


# ---------- live ----------
async def watch(args):
    import websockets
    filtres = []
    if RUNNER:
        filtres.append(f"source ayant fait >=${RUNNER:,}")
    if MIN_AGE_DAYS:
        filtres.append(f"source de plus de {MIN_AGE_DAYS:g}j")
    if MAX_CROWD_24H < 10**9:
        filtres.append(f"<={MAX_CROWD_24H} homonymes en 24h")
    print("veille pump.fun | ALERTE = image reutilisee"
          + (" | filtres : " + ", ".join(filtres) if filtres else " (aucun filtre)"), flush=True)
    n = 0
    sem = asyncio.Semaphore(8)

    async def handle(m):
        nonlocal n
        async with sem:                  # en serie on traitait 2 tokens/min : injouable
            try:
                r = await asyncio.to_thread(
                    report, m["mint"], m.get("symbol") or m.get("name"),
                    None, m.get("traderPublicKey"), m.get("uri"))
            except Exception as e:
                print(f"  ! {m.get('symbol')}: {e}", flush=True)
                return
            n += 1
            if r["alert"] or args.verbose:
                emit(r)
            if n % 25 == 0:
                save_cache()

    async for ws in websockets.connect(WSURL, ping_interval=20, open_timeout=30):
        try:
            await ws.send(json.dumps({"method": "subscribeNewToken"}))
            async for raw in ws:
                m = json.loads(raw)
                if m.get("txType") != "create" or not m.get("mint"):
                    continue
                asyncio.create_task(handle(m))
        except Exception as e:
            print(f"  ~ reconnexion ({e})", flush=True)
            save_cache()


# ---------- detecteur 2 : resurrection d'un contrat mort ----------
# Mesure fondatrice : le "blindape de mai 2025" n'a PAS couru en mai 2025. Il a
# fait son ATH de $289k le 27/08/2026, 481 jours apres sa creation -- quatre
# jours AVANT que les fermes ne sortent 19 clones en 24 secondes. Le premier
# mouvement est donc la resurrection du VIEUX contrat, pas le clone. Interet :
# il n'y a qu'UN contrat original, donc pas de loterie a 19 concurrents.

REVIVAL_MIN_AGE = 30       # jours : "mort depuis longtemps"
REVIVAL_MIN_ATH = 250_000  # a deja prouve qu'il pouvait courir
REVIVAL_MIN_MCAP = 15_000  # sous ce seuil c'est de la poussiere, pas une reprise
REVIVAL_JUMP = 1.8         # multiple du plancher observe sur la fenetre
REVIVAL_WINDOW = 900.0     # fenetre d'observation (s)
REVIVAL_LOG = os.path.join(DATA, "pump_revivals.jsonl")


def revival_scan(state, now=None):
    """Un tour de veille. state = {mint: [(t, mcap), ...]}. Rend les alertes."""
    now = now or time.time()
    d = _coins(api("/coins", limit=100, includeNsfw="true",
                   sort="last_trade_timestamp", order="DESC"))
    out = []
    for c in d:
        m = c.get("mint")
        mc = c.get("market_cap") or 0
        if not m or age_days(c, now) < REVIVAL_MIN_AGE or ath(c) < REVIVAL_MIN_ATH:
            continue
        serie = [x for x in state.get(m, []) if now - x[0] <= REVIVAL_WINDOW]
        serie.append((now, mc))
        state[m] = serie
        floor = min(v for _, v in serie) or 1
        if mc >= REVIVAL_MIN_MCAP and mc / floor >= REVIVAL_JUMP and len(serie) >= 2:
            out.append({
                "mint": m, "symbol": c.get("symbol"), "mcap": mc, "floor": floor,
                "mult": round(mc / floor, 2), "ath": ath(c),
                "age_days": round(age_days(c, now)),
                "upside": round(ath(c) / mc, 1) if mc else 0,
                "window_min": round((now - serie[0][0]) / 60, 1),
            })
            state[m] = [(now, mc)]      # re-arme, sinon l'alerte se repete
    return out


async def watch_revivals(args):
    print(f"veille resurrections | contrats >{REVIVAL_MIN_AGE}j, ATH >=${REVIVAL_MIN_ATH:,}, "
          f"mcap x{REVIVAL_JUMP} en <{REVIVAL_WINDOW / 60:.0f}min et >=${REVIVAL_MIN_MCAP:,}",
          flush=True)
    state, n = {}, 0
    while True:
        try:
            hits = await asyncio.to_thread(revival_scan, state)
        except Exception as e:
            print(f"  ~ {e}", flush=True)
            hits = []
        n += 1
        for h in hits:
            print(f"RESURRECTION {str(h['symbol'])[:14]:<14} mcap ${h['mcap']:,.0f} "
                  f"(x{h['mult']} en {h['window_min']:.0f}min) | mort depuis {h['age_days']}j | "
                  f"ATH historique ${h['ath']:,.0f} = x{h['upside']} | {h['mint']}", flush=True)
            print(f"  Ce contrat a {h['age_days']} jours et avait plafonne a ${h['ath']:,.0f}.\n"
                  f"  Son mcap vient de passer de ${h['floor']:,.0f} a ${h['mcap']:,.0f}. "
                  f"Il n'y a qu'un seul contrat : pas de clone a departager.", flush=True)
            notify(f"{h['symbol']} — contrat mort qui repart",
                   f"x{h['mult']} en {h['window_min']:.0f}min | ATH ${h['ath']:,.0f}")
            os.makedirs(DATA, exist_ok=True)
            with open(REVIVAL_LOG, "a") as f:
                f.write(json.dumps({**h, "ts": time.time()}) + "\n")
        if args.verbose and n % 4 == 0:
            print(f"  ... {len(state)} vieux contrats suivis", flush=True)
        await asyncio.sleep(args.interval)


# ---------- rejeu ----------
def replay(symbol):
    """Rejoue la regle sur tout un cluster passe : qu'aurait-elle dit, et rapporte ?"""
    twins, _ = clones_of(symbol)
    twins.sort(key=lambda c: c.get("created_timestamp") or 0)
    print(f"rejeu sur {len(twins)} homonymes exacts de '{symbol}'\n")
    print(f"{'date':<17}{'ATH $':>10} {'alerte':>7} {'plafond annonce':>16}  {'24h':>4}")
    fired, hits = [], 0
    import datetime
    for c in twins:
        t = (c.get("created_timestamp") or 0) / 1000
        if not t:
            continue
        r = report(c["mint"], now=t, coin=c)
        a = ath(c)
        if r["alert"]:
            fired.append((c, r))
            hits += a >= RUNNER
        print(f"{datetime.datetime.fromtimestamp(t, datetime.UTC):%Y-%m-%d %H:%M} "
              f"{a:>10,.0f} {'OUI' if r['alert'] else '-':>7} "
              f"{r['ceiling']:>16,.0f}  {r['n_24h']:>4}")
    n = len(fired)
    print(f"\nla regle a alerte {n} fois sur {len(twins)} lancements.")
    if n:
        big = [c for c, _ in fired if ath(c) >= RUNNER]
        med = sorted(ath(c) for c, _ in fired)[n // 2]
        print(f"  dont {len(big)} ont depasse ${RUNNER:,} => {100 * len(big) / n:.0f}% de reussite")
        print(f"  ATH median des alertes : ${med:,.0f}")
    rest = [c for c in twins if not any(c["mint"] == f["mint"] for f, _ in fired)]
    if rest:
        bigrest = [c for c in rest if ath(c) >= RUNNER]
        print(f"  non alertes : {len(rest)}, dont {len(bigrest)} >= ${RUNNER:,} (rates)")


# ---------- checks ----------
def selftest():
    """Verifie ce qui casserait silencieusement : l'empreinte d'image et les regles."""
    fp = lambda bts: (dhash(bts), thumb32(bts))

    def png(im):
        o = io.BytesIO(); im.save(o, "PNG"); return o.getvalue()

    from PIL import ImageOps
    grad = Image.new("RGB", (400, 400))
    for x in range(400):
        for y in range(400):
            grad.putpixel((x, y), (x * 255 // 399, y * 255 // 399, 128))
    reenc = io.BytesIO(); grad.resize((180, 210)).save(reenc, "JPEG", quality=55)
    autre = Image.new("RGB", (400, 400))
    for x in range(400):
        for y in range(400):
            autre.putpixel((x, y), (255 * ((x // 23 + y // 17) % 2), 40, 200 - y // 3))
    plat = Image.new("RGB", (400, 400), (128, 128, 128))

    f_ref, f_reenc = fp(png(grad)), fp(reenc.getvalue())
    ok, d = same_image(f_ref, f_reenc)
    assert ok, f"le re-encodage+resize casse l'empreinte: d={d}"

    # la canonisation miroir est VOULUE : un clone qui retourne l'image est un clone
    ok, d = same_image(f_ref, fp(png(ImageOps.mirror(grad))))
    assert ok, f"le miroir doit etre reconnu comme la meme image: d={d}"

    ok, d = same_image(f_ref, fp(png(autre)))
    assert not ok, f"deux images differentes collent: d={d}"

    # garde-fou image plate : le hash seul y produit des faux positifs
    assert rmse32(thumb32(png(plat)), thumb32(png(plat))) is None, \
        "une image unie doit rendre le verdict image impossible"
    ok, _ = same_image(fp(png(plat)), fp(png(plat)))
    assert not ok, "une image unie ne doit jamais valider une correspondance"

    assert hamming(f_ref[0], f_ref[0]) == 0
    assert ath({"ath_market_cap": 587_334_502}) == 0, "l'ATH aberrant doit etre neutralise"

    # la regle d'alerte : c'est l'IMAGE qui decide
    now = 1_800_000_000.0
    def twin(mint, img, ath_v=5_000, days=200):
        return {"mint": mint, "symbol": "X", "name": "X", "image_uri": img,
                "ath_market_cap": ath_v, "created_timestamp": (now - days * DAY) * 1000}
    def seed(lst):
        for srt in ("ath_market_cap", "created_timestamp"):
            _clones[("x", srt)] = (time.time(), lst, 0)

    empreintes = {"MOI": f_ref, "PAREILLE": f_reenc, "AUTRE": fp(png(autre))}
    vrai_img_print = globals()["img_print"]
    globals()["img_print"] = lambda u: empreintes.get(u)
    try:
        seed([twin("source", "PAREILLE")])
        r = report("neuf", "X", "MOI", "c", None, now=now)
        assert r["alert"] and r["n_same_image"] == 1, f"image reutilisee non detectee: {r}"
        assert r["img_twin"] == "source"

        seed([twin("source", "AUTRE")])
        r = report("neuf", "X", "MOI", "c", None, now=now)
        assert not r["alert"] and r["n_same_image"] == 0, f"faux positif image: {r}"

        seed([])
        assert not report("neuf", "X", "MOI", "c", None, now=now)["alert"], \
            "sans homonyme il n'y a rien a comparer"

        # les filtres optionnels doivent pouvoir couper une alerte
        global RUNNER
        seed([twin("source", "PAREILLE", ath_v=5_000)])
        RUNNER = 100_000
        try:
            assert not report("neuf", "X", "MOI", "c", None, now=now)["alert"], \
                "le filtre ATH optionnel doit couper"
        finally:
            RUNNER = 0
    finally:
        globals()["img_print"] = vrai_img_print
        for srt in ("ath_market_cap", "created_timestamp"):
            _clones.pop(("x", srt), None)

    # detecteur 2 : la regle de resurrection, sans reseau
    now2 = 1_800_000_000.0
    old = {"mint": "vieux", "symbol": "OLD", "market_cap": 40_000,
           "ath_market_cap": 900_000, "created_timestamp": (now2 - 400 * DAY) * 1000}
    import types
    saved = globals()["api"]
    globals()["api"] = lambda path, **kw: [old]
    try:
        st = {}
        old["market_cap"] = 16_000
        assert revival_scan(st, now2) == [], "premier passage : pas de plancher, pas d'alerte"
        old["market_cap"] = 40_000
        hits = revival_scan(st, now2 + 60)
        assert len(hits) == 1 and hits[0]["mult"] == 2.5, hits
        assert hits[0]["upside"] == 22.5, hits
        old["market_cap"] = 41_000
        assert revival_scan(st, now2 + 120) == [], "l'alerte ne doit pas se repeter"
        old["created_timestamp"] = (now2 - 2 * DAY) * 1000   # trop jeune
        st.clear(); old["market_cap"] = 16_000; revival_scan(st, now2)
        old["market_cap"] = 40_000
        assert revival_scan(st, now2 + 60) == [], "un token jeune n'est pas une resurrection"
    finally:
        globals()["api"] = saved
    assert ath({"ath_market_cap": 587_334_502}) == 0, "l'ATH aberrant doit etre neutralise"
    print("selftest OK - empreinte dH16+rmse (reencode, miroir, image differente, "
          "image plate) + regle age/bousculade + regle resurrection + garde-fou ATH")


def main():
    global RUNNER, MIN_AGE_DAYS, MAX_CROWD_24H
    p = argparse.ArgumentParser()
    p.add_argument("--check", metavar="MINT", help="rapport sur un mint precis")
    p.add_argument("--replay", metavar="SYMBOLE", help="rejoue la regle sur un cluster passe")
    p.add_argument("--verbose", action="store_true", help="afficher aussi les non-alertes")
    p.add_argument("--runner", type=int, default=RUNNER, help="ATH minimum d'un jumeau 'prouve' ($)")
    p.add_argument("--min-age", type=float, default=MIN_AGE_DAYS, help="age minimum du jumeau (jours)")
    p.add_argument("--max-crowd", type=int, default=MAX_CROWD_24H, help="clones max dans les 24h")
    p.add_argument("--revivals", action="store_true",
                   help="veiller les contrats morts qui repartent (detecteur 2)")
    p.add_argument("--interval", type=float, default=15.0, help="periode de scan des resurrections (s)")
    p.add_argument("--selftest", action="store_true")
    a = p.parse_args()
    RUNNER, MIN_AGE_DAYS, MAX_CROWD_24H = a.runner, a.min_age, a.max_crowd
    if a.selftest:
        return selftest()
    try:
        if a.check:
            r = report(a.check); print(line(r)); print(explain(r))
        elif a.replay:
            replay(a.replay)
        elif a.revivals:
            asyncio.run(watch_revivals(a))
        else:
            asyncio.run(watch(a))
    finally:
        save_cache()


if __name__ == "__main__":
    main()
