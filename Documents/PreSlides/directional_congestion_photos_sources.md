# "Directional Congestion in Cities" — photo sources (6 typical cities)

Real photographs only. I could not auto-download the binaries from here (the fetch tools return page text, not image files, and I'm not permitted to pull files via curl/wget). So this file holds the **sources**; download each image manually into `figs/` under the target filename listed, and the slide slot will render it.

Mix: 3 China + 3 international. Reversible-/tidal-lane operations are the physical face of peak directional congestion, so they read instantly on an opener.

| # | City | What the photo shows | Target file in `figs/` | Source (open to pick the exact frame) | License / note |
|---|------|----------------------|------------------------|----------------------------------------|----------------|
| 1 | Chengdu (成都) | 龙城大道 tidal lane; AM inbound packed, reverse light | `city_chengdu.jpg` | 光明网 https://m.gmw.cn/2025-03/06/content_1303985619.htm ; 成都日报 https://www.cdrb.com.cn/epaper/cdrbpc/202503/07/c146028.html | News photo — copyrighted; cite source or get permission for academic use |
| 2 | Beijing (北京) | 潮汐车道 / peak directional flow | `city_beijing.jpg` | 澎湃 https://www.thepaper.cn/newsDetail_forward_15565078 ; 百度百科「潮汐车道」https://baike.baidu.com/item/%E6%BD%AE%E6%B1%90%E8%BD%A6%E9%81%93/3490319 | News/encyclopedia — copyrighted; cite source |
| 3 | Shenzhen (深圳) | Your own field-visit tidal-lane photos (already in repo) | `city_shenzhen.jpg` | Reuse one of `figs/fieldvisit_crop_1.jpg … _7.jpg` | Your own photo — no license issue |
| 4 | Birmingham, UK | Aston Expressway A38(M), UK's first contraflow road (4-in/2-out AM) | `city_birmingham.jpg` | https://en.wikipedia.org/wiki/Reversible_lane | Wikipedia/Commons — usually CC-BY-SA; check the file page, attribute |
| 5 | San Francisco, USA | Golden Gate Bridge movable-median / reversible lanes | `city_sanfrancisco.jpg` | https://en.wikipedia.org/wiki/Golden_Gate_Bridge | Wikipedia/Commons — check file page license, attribute |
| 6 | Sydney, Australia | Sydney Harbour Bridge tidal-flow lanes | `city_sydney.jpg` | https://en.wikipedia.org/wiki/Sydney_Harbour_Bridge | Wikipedia/Commons — check file page license, attribute |

## Recommended for reuse
Prefer the **Wikimedia/Commons** images (#4–6) and your **own Shenzhen** photos (#3): freely usable with attribution, no permission needed. The Chinese news photos (#1–2) are copyrighted — fine to reference as a source, but for a published deck either get permission or swap for a Commons equivalent.

## Alternative "one-way jammed / other-way empty" aerials (stock, licensed)
- Getty: https://www.gettyimages.com/photos/aerial-views-of-los-angeles-rush-hour-traffic
- iStock: https://www.istockphoto.com/photos/traffic-jam-aerial
(These require a paid license; not recommended unless you buy.)

## How to wire into the slide
1. Save each image into `figs/` with the target filename above (JPG or PNG both fine — update the extension in the .tex if PNG).
2. In `tidal_advisor_revision_round2_20260703.tex`, on the "Directional Congestion in Cities" frame, replace each `\fbox{…placeholder…}` with `\includegraphics[width=\linewidth,height=0.34\textheight,keepaspectratio]{city_xxx.jpg}`.
3. Recompile. (Or drop the files in and tell me — I'll wire and recompile.)
