import requests
from datalad_crawler.consts import ARCHIVES_SPECIAL_REMOTE
from datalad_crawler.nodes.annex import Annexificator
from datalad_crawler.pipelines.simple_with_archives \
    import pipeline as sa_pipeline


def get_studytarball_urls(_):
    r = requests.get('http://anima.fz-juelich.de/api/archives')
    for url in r.json():
        yield dict(
            url=url,
        )


def pipeline():
    """Pipeline to crawl study datasets through DB queries
    """

    annex = Annexificator(
        create=False,  # must be already initialized etc
        backend='MD5E',
        statusdb='json',
        special_remotes=[ARCHIVES_SPECIAL_REMOTE],
        # every single file should go into the annex
        largefiles="anything",
    )

    incoming_pipeline = [
        get_studytarball_urls,
        annex,
    ]

    return sa_pipeline(
        annex=annex,
        rename=[':(.*)\.study:.metadata/\\1.study'],
        incoming_pipeline=incoming_pipeline,
    )
