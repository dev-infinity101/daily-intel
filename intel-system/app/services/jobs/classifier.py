import re
from dataclasses import dataclass, field

import structlog

log = structlog.get_logger(__name__)

_LEAD = re.compile(r"\b(lead|principal|staff|director|vp|head of|manager|engineering manager)\b", re.I)
_SENIOR = re.compile(r"\b(senior|sr\.?)\b", re.I)
_ENTRY = re.compile(r"\b(junior|entry.?level|associate|intern|graduate|fresher|trainee)\b", re.I)
_REMOTE = re.compile(r"\b(remote|wfh|work.?from.?home|anywhere|distributed)\b", re.I)

EV_TAXONOMY = {
    "core": [
        "EV", "E-Mobility", "Electric Vehicle", "Electrification",
        "Electric Vehicles", "E Mobility"
    ],
    "charging": [
        "Charging", "EVSE", "Charging Network", "Charging Infrastructure",
        "Charging Station", "Charging Operations", "DC Fast Charging",
        "AC Charging", "Charge Point", "CPO", "Charging Equipment",
        "Charging Solutions", "Charger Distribution"
    ],
    "last_mile": [
        "Last Mile", "Last-Mile", "Last Mile Mobility", "Last Mile Delivery",
        "Last Mile Fleet", "Last Mile Logistics", "Last Mile Partnerships"
    ],
    "business_ev": [
        "EV Infrastructure", "EV Charging", "EV Fleet", "EV Sales",
        "EV Strategy", "EV Policy", "EV Adoption", "EV Operations",
        "EV Solutions", "EV Partnerships", "Fleet Electrification",
        "Fleet Sales", "Commercial EV", "Battery Swapping", "BaaS",
        "Battery as a Service", "Sustainable Mobility", "Clean Mobility",
        "New Energy", "Mobility Services"
    ],
}

_all_ev_terms = [
    term for cluster in EV_TAXONOMY.values() for term in cluster
]
_all_ev_terms_sorted = sorted(_all_ev_terms, key=len, reverse=True)
_escaped_terms = [re.escape(t).replace(r"\ ", r"\s+") for t in _all_ev_terms_sorted]

_EV_DOMAIN_PATTERNS = re.compile(
    r"\b(" + "|".join(_escaped_terms) + r")\b",
    re.I,
)

_EV_SEMANTIC_PATTERNS = re.compile(r"(?!)", re.I)

_EV_DOMAIN = _EV_DOMAIN_PATTERNS
_EV_KEYWORDS = _EV_DOMAIN_PATTERNS
_EV_RELEVANT = _EV_DOMAIN_PATTERNS

_INDIA_LOCATION_PATTERNS = re.compile(
    r"\b("
    r"india|bharat|"
    r"maharashtra|karnataka|tamil\s+nadu|telangana|haryana|gujarat|rajasthan|uttar\s+pradesh|odisha|kerala|west\s+bengal|punjab|madhya\s+pradesh|andhra\s+pradesh|"
    r"bengaluru|bangalore|hyderabad|pune|chennai|mumbai|delhi|new\s+delhi|ncr|gurgaon|gurugram|noida|"
    r"kolkata|ahmedabad|jaipur|chandigarh|indore|bhopal|kochi|coimbatore"
    r")\b",
    re.I,
)

_NON_INDIA_LOCATION_PATTERNS = re.compile(
    r"\b("
    r"indonesia|vietnam|viet\s*nam|china|beijing|shanghai|guangzhou|thailand|"
    r"malaysia|singapore|japan|korea|philippines|united\s+states|usa|u\.s\.a\.|"
    r"us\b|california|campbell|durham|chicago|dallas|north\s+carolina|texas|"
    r"maryland|beltsville|canada|vancouver|germany|italy|bolzano|nuremberg|"
    r"munich|netherlands|amsterdam|poland|sweden|london|uk\b|europe|mexico|"
    r"brazil|australia|remote\s*\(\s*us"
    r")\b",
    re.I,
)

_REMOTE_ONLY = re.compile(
    r"^\s*(remote|multiple\s+locations|worldwide|global|anywhere|n/?a|"
    r"\[location not provided\]|not provided|unspecified)\s*$",
    re.I,
)

_TARGET_SENIORITY = re.compile(
    r"\b("
    r"chief|cbo|coo|cro|cgo|president|founder|co.?founder|vice\s+president|vp|svp|evp|avp|"
    r"director|head|general\s+manager|gm|manager|lead"
    r")\b",
    re.I,
)

_HIGH_SENIORITY = re.compile(
    r"\b("
    r"chief|cbo|coo|cro|cgo|president|founder|co.?founder|vice\s+president|vp|svp|evp|avp|"
    r"director|head|senior\s+manager|sr\.?\s+manager|general\s+manager|gm"
    r")\b",
    re.I,
)

_TARGET_FUNCTION = re.compile(
    r"\b("
    r"business|biz\s?dev|bd\b|sales|commercial|account|b2b|institutional|revenue|gtm|go.?to.?market|"
    r"strategy|strategic|growth|p&l|profit\s+(?:&|and)\s+loss|expansion|"
    r"operations?|operating|ops\b|fleet|charging|mobility|e.?mobility|ev|electric\s+vehicle|"
    r"network|delivery|baas|swapping|partnerships?|alliances?|channel|dealer|ecosystem|"
    r"projects?|programs?|product|category|supply\s+chain|procurement|customer\s+success|"
    r"transformation|innovation|pmo|regional|"
    r"solutions?|logistics|services?|electrification|enterprise|government|policy|marketing"
    r")\b",
    re.I,
)

_ENGINEERING_HEAVY_ROLES = re.compile(
    r"\b("
    r"backend|back.?end|frontend|front.?end|full.?stack|software|firmware|embedded|"
    r"developer|architect|hardware|electronics|mechanical\s+design|electrical\s+design|"
    r"design\s+engineer|systems?\s+engineer|system\s+engineer|"
    r"power\s+electronics\s+engineer|charging\s+engineer|field\s+service\s+engineer|"
    r"service\s+technician|technician|test\s+engineer|testing\s+engineer|"
    r"automation\s+engineer|validation\s+engineer|integration\s+engineer|"
    r"chassis|wiring|harness|circuit|simulation|hmi\b|cad\b|cae\b|"
    r"data\s+scientist|ai\s+engineer|ml\s+engineer|platform\s+engineer|"
    r"engineer(?:ing)?\b|it\b|information\s+technology|manufacturing\s+expert|"
    r"technical\s+specialist|core\s+technical"
    r")\b",
    re.I,
)

_ENGINEERING_WITH_BUSINESS_EXCEPTIONS = re.compile(
    r"\b("
    r"engineering\s+manager|product\s+manager|program\s+manager|project\s+manager|"
    r"technical\s+program\s+manager|cost\s+engineering\s+manager"
    r")\b",
    re.I,
)

_BUSINESS_ROLES = re.compile(
    r"\b("
    r"manager|director|head\b|vp\b|vice[\s-]president|chief\b|president|lead|leader|"
    r"principal|business\s+development|biz\s?dev|bd\b|sales|account\s+executive|"
    r"key\s+account|enterprise\s+sales|inside\s+sales|field\s+sales|channel\s+sales|"
    r"territory|regional\s+(?:head|lead)|sdr\b|bdr\b|sales\s+development|"
    r"sales\s+representative|marketing|growth\s+marketing|performance\s+marketing|"
    r"product\s+marketing|digital\s+marketing|content\s+marketing|brand\s+marketing|"
    r"demand\s+generation|partnerships|alliances|revenue|gtm\b|go.?to.?market|"
    r"commercial|pre.?sales|customer\s+success|customer\s+experience|consultant|"
    r"advisor|program\s+manager|project\s+manager|product\s+manager|operations|ops|"
    r"operation|strategy|strategist|coordinator|specialist|analyst|associate|"
    r"associates|position|positions|role|roles|supervisor|superintendent|officer|"
    r"executive|production|manufacturing|quality|procurement|supply\s+chain|"
    r"logistics|in[\s-]?charge|incharge"
    r")\b",
    re.I,
)

_SALES_BD_ROLES = re.compile(
    r"\b(sales|business\s+development|biz\s?dev|bd\b|account\s+executive|"
    r"account\s+manager|key\s+account|partnership|alliance|commercial|"
    r"pre.?sales|revenue|sdr\b|bdr\b)\b",
    re.I,
)

_MARKETING_ROLES = re.compile(
    r"\b(marketing|brand|performance\s+marketing|digital\s+marketing|"
    r"content\s+marketing|growth\s+marketing|social\s+media|pr\b|public\s+relations)\b",
    re.I,
)

_UNRELATED_BUSINESS_ROLES = re.compile(
    r"\b(hr\b|human\s+resources|recruiter|recruiting|talent\s+acquisition|payroll|"
    r"finance|accountant|accounting|bookkeeper|billing|audit|legal|counsel|"
    r"office\s+manager|admin\b|administrator|receptionist|executive\s+assistant)\b",
    re.I,
)

_HIRING_SIGNALS = re.compile(
    r"\b(we.?re\s+hiring|we\s+are\s+hiring|now\s+hiring|hiring\s+now|job\s+opening|"
    r"open\s+position|open\s+role|actively\s+hiring|join\s+our\s+team|"
    r"we.?re\s+looking\s+for|looking\s+to\s+hire|immediate\s+opening|urgent\s+opening|"
    r"career\s+opportunity|apply\s+now|send\s+your\s+cv|send\s+your\s+resume|dm\s+me|"
    r"drop\s+your\s+cv|tag\s+someone)\b",
    re.I,
)

_TAG_MAP: dict[str, re.Pattern[str]] = {
    "#Mobility": re.compile(r"\b(mobility|fleet)\b", re.I),
    "#Emobility": re.compile(r"\b(emobility|e-mobility|electric\s+mobility)\b", re.I),
    "#EV": re.compile(r"\bev\b|\belectric\s+vehicle", re.I),
    "#Charging": re.compile(r"\b(charging|charger|charge\s*point)\b", re.I),
    "#EVSE": re.compile(r"\b(evse|supply\s+equipment)\b", re.I),
    "#LastMile": re.compile(r"\b(last.?mile|last\s+mile\s+delivery)\b", re.I),
}

EV_TECHNOLOGIES: list[str] = [
    "OCPP", "OCPI", "CCS", "CHAdeMO", "ISO 15118", "J1772", "SAE J1772",
    "V2G", "V2X", "V2H", "V2B", "AC charging", "DC charging", "DCFC",
    "Wallbox", "EVMS", "BMS", "SCADA", "DERMS", "VPP", "ISO 26262",
    "AUTOSAR", "CAN bus", "LIN bus", "OBD-II", "fleet telematics",
    "smart charging", "demand response", "grid integration", "CSMS",
    "CPO", "eMSP", "roaming", "EV analytics", "energy management", "SoC", "SoH",
]
_EV_TECH_PATTERNS = [(t, re.compile(r"\b" + re.escape(t) + r"\b", re.I)) for t in EV_TECHNOLOGIES]

_DOMAINS: list[tuple[str, re.Pattern[str]]] = [
    ("EV Charging Infrastructure", re.compile(r"\b(ocpp|evse|charger|charging\s+station|cpo|csms|dcfc)\b", re.I)),
    ("Fleet & Mobility", re.compile(r"\b(fleet|ride.?hail|shared\s+mobility|dispatch|routing)\b", re.I)),
    ("Battery & Powertrain", re.compile(r"\b(battery|bms|powertrain|bev|phev|drivetrain|soc|soh)\b", re.I)),
    ("Grid & Energy", re.compile(r"\b(v2g|v2x|smart\s+grid|microgrid|vpp|demand\s+response|scada|derms)\b", re.I)),
    ("Operations & Strategy", re.compile(r"\b(operations|strategy|business\s+development|partnerships|sales)\b", re.I)),
    ("EV / Mobility (General)", re.compile(r"\b(ev|mobility|emobility|electric\s+vehicle)\b", re.I)),
]

EV_SEARCH_TERMS: list[str] = [
    "EV charging operations",
    "electric vehicle business development",
    "EVSE sales",
    "e-mobility program manager",
    "EV infrastructure product manager",
    "mobility operations",
    "charging station partnerships",
    "fleet electrification",
    "last mile EV operations",
]

_SKILLS = [
    "python", "java", "javascript", "typescript", "go", "rust", "c++", "scala", "kotlin",
    "react", "fastapi", "django", "flask", "spring", "node.js", "vue", "angular",
    "postgres", "mysql", "mongodb", "redis", "kafka", "elasticsearch",
    "aws", "gcp", "azure", "docker", "kubernetes", "terraform", "ci/cd",
    "machine learning", "deep learning", "llm", "pytorch", "tensorflow", "mlops",
    "data engineering", "data science", "spark", "airflow",
    "ocpp", "evse", "bms", "can bus", "autosar", "embedded", "firmware",
]


@dataclass(frozen=True)
class JobFilterResult:
    passed: bool
    reason: str
    score: float
    location_status: str
    domain_status: str
    role_status: str
    title_keywords: list[str] = field(default_factory=list)
    description_keywords: list[str] = field(default_factory=list)
    semantic_matches: list[str] = field(default_factory=list)


def _matches(pattern: re.Pattern[str], text: str) -> set[str]:
    return {m if isinstance(m, str) else " ".join(m) for m in pattern.findall(text)}


def infer_experience_level(title: str, description: str = "") -> str:
    text = f"{title} {description}"
    if _LEAD.search(text):
        return "lead"
    if _SENIOR.search(text):
        return "senior"
    if _ENTRY.search(text):
        return "entry"
    return "mid"


def infer_remote(location: str | None, description: str = "") -> bool | None:
    if _REMOTE.search(f"{location or ''} {description}"):
        return True
    return None


def extract_skills(title: str, description: str = "") -> list[str]:
    text = f"{title} {description}".lower()
    return [skill for skill in _SKILLS if re.search(r"\b" + re.escape(skill) + r"\b", text)]


def is_hiring_post(text: str) -> bool:
    return bool(_HIRING_SIGNALS.search(text))


def extract_ev_keywords(title: str, description: str = "") -> list[str]:
    text = f"{title} {description}"
    return [term for term, pattern in _EV_TECH_PATTERNS if pattern.search(text)]


def match_target_tags(title: str, description: str = "") -> list[str]:
    text = f"{title} {description}"
    return [tag for tag, pattern in _TAG_MAP.items() if pattern.search(text)]


def classify_job_domain(title: str, description: str = "") -> str:
    text = f"{title} {description}"
    for domain, pattern in _DOMAINS:
        if pattern.search(text):
            return domain
    return "Other"


def is_business_role(title: str, description: str = "") -> bool:
    return bool(_BUSINESS_ROLES.search(title) or _BUSINESS_ROLES.search(description))


def is_india_relevant(location: str | None = None, description: str = "") -> bool:
    return _location_status(location, description) == "india_match"


def is_preferred_role(title: str, description: str = "") -> bool:
    has_seniority = bool(_TARGET_SENIORITY.search(title))
    has_function = bool(_TARGET_FUNCTION.search(title))
    is_high_seniority = bool(_HIGH_SENIORITY.search(title))
    is_standalone_sales = bool(re.search(r"\b(sales)\b", title, re.I))
    return is_high_seniority or is_standalone_sales or (has_seniority and has_function)


def is_engineering_heavy_role(title: str, description: str = "") -> bool:
    if _ENGINEERING_WITH_BUSINESS_EXCEPTIONS.search(title):
        return False
    return bool(_ENGINEERING_HEAVY_ROLES.search(title))


def _location_status(location: str | None, description: str) -> str:
    loc = (location or "").strip().lower()
    desc = (description or "").lower()
    combined = f"{loc} {desc}"
    if _INDIA_LOCATION_PATTERNS.search(combined):
        return "india_match"
    if loc and _NON_INDIA_LOCATION_PATTERNS.search(loc):
        return "explicit_non_india"
    if loc and not _REMOTE_ONLY.match(loc):
        return "unknown_non_india_location"
    return "missing_india_signal"


def _domain_status(title: str, description: str) -> tuple[bool, str, set[str], set[str], set[str]]:
    title_text = title.lower().strip()
    desc_text = (description or "").lower().strip()
    title_kw = _matches(_EV_DOMAIN_PATTERNS, title_text)
    desc_kw = _matches(_EV_DOMAIN_PATTERNS, desc_text)
    semantic = _matches(_EV_SEMANTIC_PATTERNS, f"{title_text} {desc_text}")
    all_primary = title_kw | desc_kw

    if not all_primary:
        return False, "no_primary_ev_mobility_domain", title_kw, desc_kw, semantic
        
    status = "domain_keyword_present"
    if title_kw:
        status = "domain_keyword_in_title"
    elif len(desc_kw) >= 2:
        status = "multiple_domain_keywords_in_description"
        
    return True, status, title_kw, desc_kw, semantic


def evaluate_job_filter(
    title: str,
    description: str = "",
    location: str | None = None,
    is_target: bool = False,
    source_type: str = "",
) -> JobFilterResult:
    location_status = _location_status(location, description)
    if location_status != "india_match":
        result = JobFilterResult(False, location_status, 0.0, location_status, "not_evaluated", "not_evaluated")
        _log_filter_result(title, result, is_target)
        return result

    domain_ok, domain_status, title_kw, desc_kw, semantic = _domain_status(title, description)
    if is_target:
        domain_ok = True
        domain_status = "target_company_assumed_ev"
    elif source_type == "adzuna":
        domain_ok = True
        domain_status = "skipped_for_adzuna"

    if not domain_ok:
        result = JobFilterResult(
            False,
            domain_status,
            0.0,
            location_status,
            domain_status,
            "not_evaluated",
            sorted(title_kw),
            sorted(desc_kw),
            sorted(semantic),
        )
        _log_filter_result(title, result, is_target)
        return result

    preferred = is_preferred_role(title, description)
    engineering_heavy = is_engineering_heavy_role(title, description)
    unrelated = bool(_UNRELATED_BUSINESS_ROLES.search(title))

    if engineering_heavy:
        role_status = "engineering_heavy_role"
        passed = False
    elif unrelated:
        role_status = "unrelated_business_role"
        passed = False
    elif not preferred:
        role_status = "not_target_business_role"
        passed = False
    else:
        role_status = "target_role"
        passed = True

    score = _compute_filter_score(title_kw, desc_kw, semantic, location_status, domain_status, role_status)
    result = JobFilterResult(
        passed,
        "accepted" if passed else role_status,
        score if passed else 0.0,
        location_status,
        domain_status,
        role_status,
        sorted(title_kw),
        sorted(desc_kw),
        sorted(semantic),
    )
    _log_filter_result(title, result, is_target)
    return result


def _compute_filter_score(
    title_kw: set[str],
    desc_kw: set[str],
    semantic: set[str],
    location_status: str,
    domain_status: str,
    role_status: str,
) -> float:
    if location_status != "india_match" or role_status != "target_role":
        return 0.0
    score = 0.25
    score += min(0.25, 0.08 * len(title_kw))
    score += min(0.20, 0.05 * len(desc_kw))
    score += min(0.10, 0.02 * len(semantic))
    if domain_status == "domain_keyword_in_title":
        score += 0.15
    elif domain_status == "multiple_domain_keywords_in_description":
        score += 0.08
    score += 0.15
    return min(1.0, round(score, 2))


def _log_filter_result(title: str, result: JobFilterResult, is_target: bool) -> None:
    log.info(
        "job_filter.evaluate",
        title=title[:80],
        passed=result.passed,
        reason=result.reason,
        score=result.score,
        location_status=result.location_status,
        domain_status=result.domain_status,
        role_status=result.role_status,
        title_keywords=result.title_keywords,
        desc_keywords=result.description_keywords,
        semantic=result.semantic_matches,
        is_target=is_target,
    )


def score_ev_relevance(title: str, description: str = "", location: str | None = None) -> float:
    return evaluate_job_filter(title, description, location).score


def is_ev_domain_relevant(
    title: str,
    description: str = "",
    is_target: bool = False,
    location: str | None = None,
) -> bool:
    return is_ev_relevant(title, description, is_target=is_target, location=location)


def passes_jobs_filter(title: str, description: str = "") -> bool:
    return is_ev_relevant(title, description)


def is_ev_relevant(
    title: str,
    description: str = "",
    is_target: bool = False,
    location: str | None = None,
) -> bool:
    return evaluate_job_filter(title, description, location, is_target=is_target).passed
