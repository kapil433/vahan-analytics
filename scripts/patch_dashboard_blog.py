"""Replace Events/Intel/Upload block in dashboard with Blog HTML. Run from repo root if needed."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
path = ROOT / "api/static/dashboard/index.html"
text = path.read_text(encoding="utf-8")
start = text.index("<!-- EVENTS PAGE -->")
end = text.index('  <div class="page" id="page-info">')

BLOG = r"""<!-- BLOG — articles grounded in the 47 timeline events in the VAHAN master bundle -->
<div class="page" id="page-blog">
  <div style="margin-bottom:20px">
    <div style="font-size:11px;color:var(--text-dim);letter-spacing:2px;margin-bottom:4px">INSIGHTS</div>
    <div style="font-size:20px;font-weight:700;color:var(--text)">Blog — from the market events timeline</div>
    <div style="font-size:12px;color:var(--text-dim);margin-top:6px;max-width:760px">Short reads derived from the same research-tagged events used in this product (policy, shocks, subsidies, and data breaks). They explain how to interpret registration charts without mistaking <strong>rules, taxes, or geography</strong> for pure demand moves. Always cross-check critical numbers with official SIAM, MoRTH, PIB, and RTO sources.</div>
  </div>
  <div class="blog-list">
    <article itemscope itemtype="https://schema.org/Article">
      <h2 itemprop="headline">Telangana formation: when a state split looks like a demand crash</h2>
      <div class="blog-meta"><time itemprop="datePublished" datetime="2014-06-02">June 2014</time> · Data break · Tier 1</div>
      <p itemprop="description">On 2 June 2014, Telangana separated from Andhra Pradesh. Hyderabad, Rangareddy, and related RTOs—roughly 45–53% of undivided AP registrations—moved to the new state. Legacy &quot;Andhra Pradesh&quot; time series show a structural step-down that is <strong>not</strong> a collapse in car buying; it is a boundary change in what geography the label covers.</p>
      <p class="blog-reading"><strong>Reading the data:</strong> avoid YoY comparisons that straddle June 2014 for old AP labels; use consistent post-split geographies or All India.</p>
      <p class="blog-sources"><strong>Sources:</strong> Andhra Pradesh Reorganisation Act 2014 · Vahan RTO coverage change (Jun 2014) · FADA AP state data</p>
    </article>

    <article itemscope itemtype="https://schema.org/Article">
      <h2 itemprop="headline">Diesel from deregulation to BS6: why PV diesel share fell for years</h2>
      <div class="blog-meta"><time itemprop="datePublished" datetime="2014-10-01">2014–2023</time> · Policy &amp; regulation · Structural trend</div>
      <p itemprop="description">October 2014 ended diesel price administration; diesel lost much of its rupee-per-km edge over petrol by 2015. BS6 from April 2020 raised compliance cost; Maruti Suzuki and Honda exiting diesel removed a large share of diesel SKUs in one stroke. Together these explain a <strong>multi-year</strong> fall in diesel share of passenger registrations—not a single-month blip.</p>
      <p class="blog-reading"><strong>Reading the data:</strong> separate one-off BS4/BS6 inventory spikes from the slower share shift visible in SIAM-style fuel mix.</p>
      <p class="blog-sources"><strong>Sources:</strong> MoPNG (Oct 2014) · PPAC price data · Maruti press statement / FY20 report · SIAM fuel-type series</p>
    </article>

    <article itemscope itemtype="https://schema.org/Article">
      <h2 itemprop="headline">EV subsidies: FAME I and II, the 2024 cliff, EMPS, and PM E-DRIVE</h2>
      <div class="blog-meta"><time itemprop="datePublished" datetime="2015-04-01">2015–2024</time> · Subsidy sequence</div>
      <p itemprop="description">FAME I (April 2015) seeded a tiny EV base. FAME II (April 2019, ₹10,000 crore) aligned with credible four-wheel EV launches and visible step-up in volumes. FAME II&apos;s March 2024 end produced a classic <strong>front-load then cliff</strong>; EMPS (April 2024) limited the gap, then PM E-DRIVE (October 2024, ₹10,900 crore) set the next policy horizon.</p>
      <p class="blog-reading"><strong>Reading the data:</strong> expect Feb–Mar spikes and April softness around scheme boundaries; compare to pre-subsidy trend, not only prior month.</p>
      <p class="blog-sources"><strong>Sources:</strong> MHI FAME I/II notifications · PIB · MHI EMPS Apr 2024 · PIB PM E-DRIVE Oct 2024 · SMEV commentary</p>
    </article>

    <article itemscope itemtype="https://schema.org/Article">
      <h2 itemprop="headline">March–April 2020: BS4 clearance collided with the COVID lockdown</h2>
      <div class="blog-meta"><time itemprop="datePublished" datetime="2020-03-23">Mar–Apr 2020</time> · Overlapping shocks · Tier 1</div>
      <p itemprop="description">BS6 enforcement from 1 April 2020 forced a March rush to register remaining BS4 stock. Days later, the national lockdown (23 March onward) drove April registrations toward zero. The overlap is one of the largest distortions in many VAHAN series—<strong>high YoY growth</strong> right after can be mostly base effects, not organic acceleration.</p>
      <p class="blog-reading"><strong>Reading the data:</strong> use level vs trend and avoid naive YoY% for months anchored to April 2020.</p>
      <p class="blog-sources"><strong>Sources:</strong> Supreme Court / MoRTH BS6 timeline · MHA lockdown order Mar 2020 · SIAM · FADA Mar–Apr 2020 retail</p>
    </article>

    <article itemscope itemtype="https://schema.org/Article">
      <h2 itemprop="headline">April–July 2017: BS4, pre-GST inventory, and GST go-live in one window</h2>
      <div class="blog-meta"><time itemprop="datePublished" datetime="2017-04-01">Apr–Jul 2017</time> · Composite regulation / tax</div>
      <p itemprop="description">April 2017 brought nationwide BS4 for the relevant segments; June saw dealers clearing stock ahead of GST; July introduced new invoicing and short operational disruption. Registration bumps and dips in this window blend <strong>emission rules, tax incidence, and reporting</strong>—they cannot be cleanly attributed to a single lever in the chart.</p>
      <p class="blog-reading"><strong>Reading the data:</strong> treat Apr–Jul 2017 as one annotated band in your head; prefer adjacent clean quarters for OEM or fuel attribution.</p>
      <p class="blog-sources"><strong>Sources:</strong> MoRTH BS4 notification · GST Council Jun–Jul 2017 · SIAM / FADA Apr–Jul 2017 · GST cess schedule on large diesel cars</p>
    </article>

    <article itemscope itemtype="https://schema.org/Article">
      <h2 itemprop="headline">CNG: city gas expansion, then the 2021–22 price shock</h2>
      <div class="blog-meta"><time itemprop="datePublished" datetime="2018-01-01">2018–2022</time> · Infrastructure + macro shock</div>
      <p itemprop="description">PNGRB&apos;s 9th CGD round and later build-out expanded CNG into many new districts—CNG car registrations tend to follow <strong>station availability</strong>. From 2021, global energy stress (including Russia–Ukraine) fed through to sharply higher CNG tariffs in several cities; growth <strong>rates</strong> cooled even where CNG retained total-cost advantage vs petrol.</p>
      <p class="blog-reading"><strong>Reading the data:</strong> distinguish level (still often cheaper km-cost) from YoY registration momentum.</p>
      <p class="blog-sources"><strong>Sources:</strong> PNGRB CGD rounds · PPAC / CGD reports · IGL/MGL price notifications 2022 · SIAM CNG monthly</p>
    </article>

    <article itemscope itemtype="https://schema.org/Article">
      <h2 itemprop="headline">When registrations understate demand: NBFC stress and the chip shortage</h2>
      <div class="blog-meta"><time itemprop="datePublished" datetime="2018-09-01">2018–2022</time> · Credit &amp; supply constraints</div>
      <p itemprop="description">The IL&amp;FS default (September 2018) tightened NBFC liquidity; two-wheelers and entry segments financed outside prime bank channels were hit hardest. From mid-2021, a global semiconductor shortage capped OEM output—retail waitlists rose while VAHAN counts looked soft. Easing from late 2022 mainly reflected <strong>supply catching backlog</strong>, not a pure demand surge.</p>
      <p class="blog-reading"><strong>Reading the data:</strong> pair registration series with SIAM wholesale or OEM commentary when supply binds.</p>
      <p class="blog-sources"><strong>Sources:</strong> RBI FSR Dec 2018 · Maruti / Hyundai investor calls 2021 · SIAM supply alerts · ACMA / OEM Q3 FY23 commentary</p>
    </article>

    <article itemscope itemtype="https://schema.org/Article">
      <h2 itemprop="headline">Cab aggregators: fleet registrations as a proxy, not ride demand</h2>
      <div class="blog-meta"><time itemprop="datePublished" datetime="2014-01-01">2014–2018</time> · Product / fleet cycle</div>
      <p itemprop="description">Ola and Uber scaling in 2014–2017 lifted motor-cab class registrations (often CNG or diesel) in several cities—visible when you slice commercial or geography correctly. From 2018, incentive cuts hit driver economics and <strong>new partner additions</strong> slowed; fleet registration momentum eased. This is a structural fleet cycle, not the same as app trip growth.</p>
      <p class="blog-reading"><strong>Reading the data:</strong> use motor-cab or state-level series; do not map directly to All India PV passenger totals.</p>
      <p class="blog-sources"><strong>Sources:</strong> FADA motor cab data 2014–18 · IDC driver survey 2018 · SIAM</p>
    </article>
  </div>
</div>

"""

path.write_text(text[:start] + BLOG + text[end:], encoding="utf-8")
print("Patched", path)
