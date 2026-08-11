from news_fetcher.relevance import deduplicate, is_upsc_relevant, same_story


def test_filters_party_politics_but_keeps_governance():
    assert not is_upsc_relevant({"publisher": "Paper", "title": "Party rally begins campaign trail", "excerpt": ""})
    assert is_upsc_relevant({"publisher": "Paper", "title": "Supreme Court reviews election law", "excerpt": ""})
    assert not is_upsc_relevant({
        "publisher": "Times of India",
        "title": "'Counting fingers, not hearts': Shehzad Poonawalla mocks Rahul's Gen Z post",
        "excerpt": "Rahul Gandhi shared a social media gesture and a spokesperson accused him of corruption.",
    })
    assert is_upsc_relevant({
        "publisher": "Paper",
        "title": "Rahul Gandhi speaks as Parliament debates constitutional amendment bill",
        "excerpt": "The proposed law changes governance rules.",
    })


def test_deduplicates_and_prefers_digest():
    rss = {"publisher": "Paper", "title": "Cabinet approves new national education scheme", "article_url": "https://a"}
    digest = {"publisher": "Perplexity Digest", "title": "Cabinet approves national education scheme", "article_url": "https://b", "sources": ["https://b"]}
    assert same_story(rss, digest)
    result = deduplicate([rss, digest])
    assert len(result) == 1
    assert result[0]["publisher"] == "Perplexity Digest"
    assert "https://a" in result[0]["sources"]
