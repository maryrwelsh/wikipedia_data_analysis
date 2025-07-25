-- This SQL creates or replaces the CLASSIFY_WIKIPEDIA_PAGE UDF in Snowflake.
CREATE OR REPLACE FUNCTION CLASSIFY_WIKIPEDIA_PAGE(page_title VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'mixtral-8x7b', -- Or 'llama2-70b', 'gemma-7b', etc. based on availability and preference
        'Categorize the Wikipedia page title "' || page_title || '" into one of these categories: ' ||
        'Technology, History, Science, Sports, Arts & Culture, Geography, Politics, Current Events, ' ||
        'Biography, Health, Nature, Entertainment, Miscellaneous. ' ||
        'Return only the category name as a single word string with no further explanation. ' ||
        'If it fits multiple, pick the most prominent one. If it doesn''t fit any, use "Miscellaneous".'
    )
$$;