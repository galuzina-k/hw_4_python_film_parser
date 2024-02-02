import scrapy


class FilmSpiderSpider(scrapy.Spider):
    name = "film_spider"
    allowed_domains = ["ru.wikipedia.org"]
    start_urls = ["https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту"]

    def __init__(self):

        #   #   #   #   #   #
        self.MAX_PAGE_CNT = 100  # SET BY USER
        #   #   #   #   #   #

        self.PAGE_CNT = 0

        self.TOTAL_FILMS = 0
        self.FILMS_ALREADY_PROCESSED = 0
        super().__init__()

    def parse(self, response):
        bunch_of_urls = response.xpath("//div[@class='mw-category mw-category-columns']").xpath(".//a")
        list_of_urls = []
        for a_block in bunch_of_urls:
            url = a_block.xpath(".//@href").get()
            list_of_urls.append("https://ru.wikipedia.org" + url)

        self.TOTAL_FILMS += len(list_of_urls)

        next_page_link = "https://ru.wikipedia.org" + response.xpath("//a[contains(text(), 'Следующая страница')]").xpath(".//@href").get()

        if not self.PAGE_CNT > self.MAX_PAGE_CNT:

            if self.PAGE_CNT % 5 == 0:
                print("Started working on page {0}".format(self.PAGE_CNT))
            self.PAGE_CNT += 1

            for url in list_of_urls:
                yield scrapy.Request(url=url, callback=self.response_parser)

            yield scrapy.Request(url=next_page_link, callback=self.parse)

    def response_parser(self, response):
        # Название фильма
        title = response.xpath("//span[@class='mw-page-title-main']").xpath(".//text()").get()
        # Страна
        country = response.xpath("//th[contains(text(), 'Стран')]/following-sibling::td").xpath(".//a//text()").get()
        # Жанр
        genre = response.xpath("//span[@data-wikidata-property-id='P136']").xpath(".//text()").get()
        # Режиссер
        director = response.xpath("//th[contains(text(), 'Режиссёр')]/following-sibling::td").xpath(".//a//text()").get()
        # Год
        year = response.xpath("//th[text()='Год']/following-sibling::td").xpath(".//a//text()").getall()
        if len(year) == 0:
            try:
                year = response.xpath("//th[text()='Год']/following-sibling::td").xpath(".//text()").getall()[-1]
            except IndexError:
                year = None
        else:
            year = year[-1]

        if self.FILMS_ALREADY_PROCESSED % 500 == 0:
            print("Processed {0} films out of {1}".format(self.FILMS_ALREADY_PROCESSED, self.TOTAL_FILMS))
        self.FILMS_ALREADY_PROCESSED += 1
        yield {
            'title': title,
            'country': country,
            'genre': genre,
            "director": director,
            "year": year
        }
