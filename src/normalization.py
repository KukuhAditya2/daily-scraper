from cleantext import clean


def filter_text(text: str) -> str:
    cleaned_content = clean(
        text,
        fix_unicode=True,
        to_ascii=False,
        lower=False,
        no_line_breaks=True,
        normalize_whitespace=True,
        no_emoji=True,
        no_urls=False,
        no_emails=False,
        no_phone_numbers=False,
        no_numbers=False,
        no_digits=False,
        no_currency_symbols=False,
        no_punct=False,
        lang="en"
    )

    return cleaned_content
