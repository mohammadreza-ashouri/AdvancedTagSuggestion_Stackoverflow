from rake_nltk import Rake
from tokenize import tokenize, untokenize, NUMBER, STRING, NAME, OP
from io import BytesIO
import keyword


def get_text_keyword(text):

    rake = Rake()
    rake.extract_keywords_from_text(text=text)

    return rake.get_ranked_phrases()


def get_text_keyword_with_scores(text):

    rake = Rake()
    rake.extract_keywords_from_text(text=text)

    return rake.get_ranked_phrases_with_scores()


def get_code_keywords(text):
    tokens = []
    try:
        g = tokenize(BytesIO(text.encode('utf-8')).readline)
        for toknum, tokval, _, _, _ in g:
            if toknum == NAME and not keyword.iskeyword(tokval):
                if tokval not in tokens:
                    tokens.append(tokval)
    except:
        pass

    return tokens