from hypothesis import strategies as st

from . import tess as tess_st


@st.composite
def _unix_safe_str(draw, **text_params):
    return draw(
        st.text(alphabet=st.characters(whitelist=["L", "N"]), **text_params)
    )


@st.composite
def filename(draw, extension=None):
    core = draw(_unix_safe_str(min_size=1))
    suffix = (
        draw(st.just(extension))
        if extension
        else draw(_unix_safe_str(min_size=1, max_size=5))
    )
    return f"{core}.{suffix}"
