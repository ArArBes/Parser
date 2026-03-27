from asyncio import sleep, gather, Semaphore, run
from random import uniform
from re import sub
from pandas import DataFrame
from playwright.async_api import async_playwright, Page, BrowserContext
from typing import Dict, Any, Union


class WBParser:
    def __init__(self):
        """Инициализация парсера: задаёт базовый URL поиска,
        множество уже обработанных URL и список всех найденных товаров.
        """
        self.base_url_search = 'https://www.wildberries.ru/catalog/0/search.aspx?search='
        self.seen_urls = set()
        self.all_products = []

    async def search(self, query: str) -> None:
        """
        Запускает поиск по запросу: открывает браузер, переходит на страницу поиска,
        применяет фильтры, собирает ссылки на товары и сохраняет результаты.
        """
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                ],
            )

            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            )

            page = await context.new_page()
            await page.goto(self.base_url_search + query)
            await sleep(2)

            await self.apply_filters(page)
            await sleep(2)

            try:
                total_text = await page.locator("span.searching-results__count").inner_text()
                total = int(sub(r"\D", "", total_text)) if total_text else 0
            except:
                total = 100

            await self.scroll_and_collect(page, context, max_count=total)
            await browser.close()
            self.save_results()

    async def apply_filters(self, page: Page) -> None:
        """
        Применяет фильтры на странице поиска:
        открывает блок фильтров, ставит верхнюю границу цены до 10 000 ₽,
        выбирает страну производства «Россия» и нажимает «Показать».
        """
        try:
            await page.click("button.dropdown-filter__btn--all")
            price_input_end = page.locator("input[name='endN']")
            await price_input_end.fill("")
            await price_input_end.fill("10000")
            await price_input_end.press("Enter")

            russia_checkbox = page.locator("div.checkbox-with-text.j-list-item.brand-filter-logo", has_text="Россия")
            selected = await russia_checkbox.get_attribute("class")
            if "selected" not in selected:
                await russia_checkbox.click()

            show_button = page.locator("button.filters-desktop__btn-main.btn-main", has_text="Показать")
            await show_button.click()
        except Exception as e:
            pass

    async def scroll_and_collect(self, page: Page, context: BrowserContext, max_count: int = 1000):
        """
        Скроллит страницу поиска вниз, собирает новые ссылки на карточки товаров
        и параллельно парсит их в карточках, пока не достигнут лимит по количеству.
        """
        processed = set()
        sem = Semaphore(5)
        last_height = 0
        no_new_products_count = 0

        while no_new_products_count < 30 and len(self.all_products) < max_count:
            product_links = page.locator("a.product-card__link")
            current_count = await product_links.count()

            new_links = []
            for i in range(current_count):
                href = await product_links.nth(i).get_attribute("href")
                if href and href not in processed:
                    processed.add(href)
                    full_url = f"https://www.wildberries.ru{href}" if href.startswith('/') else href
                    new_links.append(full_url)

            if new_links:
                async def parse(link):
                    async with sem:
                        return await self.parse_card(context, link)

                batch_tasks = [parse(link) for link in new_links]
                results = await gather(*batch_tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, dict) and result:
                        self.all_products.append(result)

                no_new_products_count = 0
            else:
                no_new_products_count += 1

            await page.evaluate("window.scrollBy(0, 1000)")
            await sleep(2)

            new_height = await page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                no_new_products_count += 1
            last_height = new_height

    async def parse_card(self, context: BrowserContext, url: str) -> Dict[str, Any]:
        """
        Открывает карточку товара по ссылке,
        извлекает все нужные данные и возвращает словарь с информацией.
        """
        page = await context.new_page()
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=15000)
            await sleep(uniform(1, 2))

            try:
                close_btn = page.locator("button.btnDetail--im7UR")
                if await close_btn.count() > 0:
                    await close_btn.first.click()
            except:
                pass

            rating_data = await self._get_rating(page)
            if rating_data['рейтинг'] < 4.5:
                return {}

            price = await self._get_price(page)
            data = {
                "url": url,
                "артикул": await self._get_article(page),
                "название": await self._get_name(page),
                "цена": price,
                "описание": await self._get_description(page),
                "изображения": await self._get_images(page),
                "характеристики": await self._get_specs(page),
                "продавец": await self._get_seller(page),
                "ссылка_на_продавца": await self._get_seller_link(page),
                "размеры": await self._get_sizes(page),
                "остатки_общие": await self._get_stocks(page),
                "рейтинг": 0,
                "количество_отзывов": 0,
            }
            data.update(rating_data)
            return data
        except Exception as e:
            return {}
        finally:
            await page.close()

    async def _get_article(self, page: Page) -> str:
        locator = page.locator("td.cellValue--hHBJB")
        if await locator.count() > 0:
            return (await locator.first.inner_text()).strip()
        return ""

    async def _get_name(self, page: Page) -> str:
        locator = page.locator("h2.productTitle--lfc4o")
        if await locator.count() > 0:
            return (await locator.first.inner_text()).strip()
        return ""

    async def _get_price(self, page: Page) -> int:
        locator = page.locator("span.priceBlockPrice--xf8pi h2")
        if await locator.count() > 0:
            price_str = await locator.first.inner_text()
            price = ''.join(c for c in price_str if c.isdigit())
            return int(price) if price else 0
        return 0

    async def _get_description(self, page: Page) -> str:
        locator = page.locator("p.descriptionText--Jq9n2")
        if await locator.count() > 0:
            return (await locator.first.inner_text()).strip()
        return ""

    async def _get_images(self, page: Page) -> str:
        images = []
        imgs = page.locator("img[alt^='Product image']")
        count = await imgs.count()
        for i in range(min(count, 10)):
            src = await imgs.nth(i).get_attribute("src")
            if src:
                images.append(src)
        return ", ".join(images)

    async def _get_specs(self, page: Page) -> Dict[str, Dict[str, str]]:
        specs = {}
        try:
            section = page.locator("section[data-testid='product_additional_information']")
            tables = section.locator("table")
            tables_count = await tables.count()

            for t in range(tables_count):
                table = tables.nth(t)
                try:
                    section_name = (await table.locator("caption").inner_text()).strip()
                except:
                    section_name = f"section_{t}"

                section_items = {}
                rows = table.locator("tr")
                rows_count = await rows.count()

                for i in range(rows_count):
                    row = rows.nth(i)
                    try:
                        key_locator = row.locator("th span")
                        if await key_locator.count() > 0:
                            key = (await key_locator.first.inner_text()).strip()
                        else:
                            continue

                        value_locator = row.locator("td div")
                        if await value_locator.count() > 0:
                            value = (await value_locator.first.inner_text()).strip()
                        else:
                            continue

                        if key and value:
                            section_items[key] = value
                    except:
                        pass

                if section_items:
                    specs[section_name] = section_items
        except:
            pass
        return specs

    async def _get_seller(self, page: Page) -> str:
        locator = page.locator("span.sellerInfoNameDefaultText--qLwgq")
        if await locator.count() > 0:
            return (await locator.first.inner_text()).strip()
        return ""

    async def _get_seller_link(self, page: Page) -> str:
        locator = page.locator("a.sellerInfoButtonLink--RoLBz")
        if await locator.count() > 0:
            href = await locator.first.get_attribute("href")
            if href:
                return f"https://www.wildberries.ru{href}"
        return ""

    async def _get_sizes(self, page: Page) -> str:
        sizes = []
        size_elems = page.locator("span.sizesListSize--NUoNC")
        count = await size_elems.count()
        for i in range(count):
            size = await size_elems.nth(i).inner_text()
            if size:
                sizes.append(size.strip())
        return ", ".join(sizes)

    async def _get_stocks(self, page: Page) -> int:
        try:
            stocks = page.locator(".size-selector__size")
            count = await stocks.count()
            available = 0
            for i in range(count):
                is_available = await stocks.nth(i).get_attribute("data-available")
                if is_available == "true":
                    available += 1
            return available
        except:
            return 0

    async def _get_rating(self, page: Page) -> Dict[str, Union[float, int]]:
        locator = page.locator("span.productReviewRating--PD7fr")
        if await locator.count() > 0:
            text = (await locator.first.inner_text()).strip()
            parts = text.split(" · ")
            if len(parts) == 2:
                rating = parts[0].replace(',', '.')
                reviews = parts[1].split()[0]
                return {
                    "рейтинг": float(rating) if rating else 0,
                    "количество_отзывов": int(reviews) if reviews.isdigit() else 0
                }
        return {"рейтинг": 0, "количество_отзывов": 0}

    def save_results(self) -> None:
        """
        Фильтрует собранные товары по рейтингу более 4.5, цене менее 10 000 и стране производства «Россия»,
        затем сохраняет результат в Excel-файл.
        """
        if not self.all_products:
            return

        filtered = []
        for p in self.all_products:
            rating = p.get("рейтинг", 0)
            price = p.get("цена", 0)
            specs = p.get("характеристики", {})

            country = ""
            for section in specs.values():
                if isinstance(section, dict):
                    country = section.get("Страна производства", "")
                    if country:
                        break

            if rating >= 4.5 and price <= 10000 and "Россия" in country:
                filtered.append(p)

        if filtered:
            df_filtered = DataFrame(filtered)
            df_filtered.to_excel("wildberries_palto_filtered.xlsx", index=False, engine='openpyxl')
