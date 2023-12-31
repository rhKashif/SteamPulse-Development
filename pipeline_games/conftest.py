"""File containing necessary pytest fixtures for testing"""
import pytest
import pandas as pd


@pytest.fixture
def fake_html() -> str:
    """Fake html scraping for testing"""
    return """<a class="search_result_row ds_collapse_flag" data-ds-appid="12345"
                data-ds-itemkey="App_2501550" data-ds-steam-deck-compat-handled="true" 
                data-ds-tagids="[4255,4885,4637,19,12057,4026,1774]" data-gpnav="item"
                data-search-page="1" href="https://store.steampowered.com/app/2501550/Bullet_Hell_Monday_Finale/?snr=1_7_7_230_150_1"
                onmouseout="HideGameHover( this, event, 'global_hover' )" onmouseover="GameHover( this, event, 'global_hover',
                {&quot;type&quot;:&quot;app&quot;,&quot;id&quot;:2501550,&quot;public&quot;:1,&quot;v6&quot;:1} );">
                <div class="col search_capsule"><img src="https://cdn.cloudflare.steamstatic.com/steam/apps/2501550/capsule_sm_120.jpg?t=1693793997" 
                srcset="https://cdn.cloudflare.steamstatic.com/steam/apps/2501550/capsule_sm_120.jpg?t=1693793997 
                1x, https://cdn.cloudflare.steamstatic.com/steam/apps/2501550/capsule_231x87.jpg?t=1693793997 2x"/></div>
                <div class="responsive_search_name_combined">
                <div class="col search_name ellipsis">
                <span class="title">SteamPulse: FAKE GAME</span>
                <div>
                <span class="platform_img win"></span> </div>
                </div>
                <div class="col search_released responsive_secondrow">5 Sep, 2023</div>
                <div class="col search_reviewscore responsive_secondrow">
                </div>
                <div class="col search_price_discount_combined responsive_secondrow" data-price-final="0">
                <div class="col search_discount_and_price responsive_secondrow">
                </div>
                </div>
                </div>
                <div style="clear: left;"></div>
                </a>"""


@pytest.fixture
def fake_html_soup() -> str:
    """Fake html after it has been passed through soup"""
    return """<html>
        <body>
        <div class="glance_ctn_responsive_right" data-panel='{"flow-children":"column"}' id="glanceCtnResponsiveRight">
        <!-- when the javascript runs, it will set these visible or not depending on what fits in the area -->
        <div class="responsive_block_header">Tags</div>
        <div class="glance_tags_ctn popular_tags_ctn" data-panel='{"flow-children":"row"}'>
        <div class="glance_tags_label">Popular user-defined tags for this product:</div>
        <div class="glance_tags popular_tags" data-appid="2156440" data-panel='{"flow-children":"row"}'>
        <a class="app_tag" href="https://store.steampowered.com/tags/en/RPG/?snr=1_5_9__409" style="display: none;">
                                                                                                        Fake_Tag 1                                                             </a><a class="app_tag" href="https://store.steampowered.com/tags/en/Strategy/?snr=1_5_9__409" style="display: none;">
                                                                                                        Fake_Tag 2                                                         </a><a class="app_tag" href="https://store.steampowered.com/tags/en/MMORPG/?snr=1_5_9__409" style="display: none;">
                                                                                                        Fake_Tag 3                                                            </a>
        </div>
        <div class="responsive_block_header" id="reviewsHeader_responsive" style="display: none;">Reviews</div>
        <div class="user_reviews" data-panel='{"focusable":true,"clickOnActivate":true}' id="userReviews_responsive" onclick="window.location='#app_reviews_hash'" style="display: none;">
        <div class="user_reviews_summary_row" id="appReviewsAll_responsive" onclick="window.location='#app_reviews_hash'" style="cursor: pointer;">
        <div class="subtitle column all">All Reviews:</div>


        <div class="game_area_purchase_platform"><span class="platform_img win"></span><span class="platform_img mac"></span><span class="platform_img linux"></span></div>
        <h1>Buy Raiding.Zone</h1>
        <div class="game_purchase_action">
        <div class="game_purchase_action_bg">
        <div class="game_purchase_price price" data-price-final="169">
                                                                £1.69                                           </div>
        <div class="btn_addtocart">

        </body>
        </html>"""


@pytest.fixture
def html_no_tags() -> str:
    """Fake html containing tag and price data"""
    return """<html>
        <body>
        <div class="glance_ctn_responsive_right" data-panel='{"flow-children":"column"}' id="glanceCtnResponsiveRight">
        <!-- when the javascript runs, it will set these visible or not depending on what fits in the area -->
        <div class="responsive_block_header">Tags</div>
        <div class="glance_tags_ctn popular_tags_ctn" data-panel='{"flow-children":"row"}'>
        <div class="glance_tags_label">Popular user-defined tags for this product:</div>
        <div class="glance_tags popular_tags" data-appid="2156440" data-panel='{"flow-children":"row"}'>
        <div class="responsive_block_header" id="reviewsHeader_responsive" style="display: none;">Reviews</div>
        <div class="user_reviews" data-panel='{"focusable":true,"clickOnActivate":true}' id="userReviews_responsive" onclick="window.location='#app_reviews_hash'" style="display: none;">
        <div class="user_reviews_summary_row" id="appReviewsAll_responsive" onclick="window.location='#app_reviews_hash'" style="cursor: pointer;">
        <div class="subtitle column all">All Reviews:</div>


        <div class="game_area_purchase_platform"><span class="platform_img win"></span><span class="platform_img mac"></span><span class="platform_img linux"></span></div>
        <h1>Buy Raiding.Zone</h1>
        <div class="game_purchase_action">
        <div class="game_purchase_action_bg">
        <div class="discount_original_price" data-price-final="250">
                                                                £2.50                                           </div>
        <div class="discount_final_price" data-price-final="169">
                                                                £1.69                                           </div>
        <div class="btn_addtocart">

        </body>
        </html>"""


@pytest.fixture
def fake_response() -> dict:
    """Fake API response"""
    return {'type': 'game', 'name': 'Just fighting', 'steam_appid': 2545170, 'required_age': 0,
            'is_free': False, 'developers': ['Fake Developer 1', 'Fake Developer 2'],
            'publishers': ['Fake Publisher'], 'price_overview':
            {'currency': 'GBP', 'initial': 249, 'final': 249, 'discount_percent': 0,
                'initial_formatted': '', 'final_formatted': '£2.49'},
            'packages': [914324], 'platforms': {'windows': True, 'mac': False, 'linux': False},
            'categories': [{'id': 2, 'description': 'Single-player'}],
            'genres': [{'id': '1', 'description': 'Action'},
                       {'id': '25', 'description': 'Adventure'},
                       {'id': '28', 'description': 'Simulation'},
                       {'id': '2', 'description': 'Strategy'}],
            'release_date': {'coming_soon': False, 'date': '5 Sep, 2023'}}


@pytest.fixture
def fake_raw_data() -> pd.DataFrame:
    """Fake raw data from pretend dataframe"""
    return pd.DataFrame([[2246030, "Fake: Escape", "5 Sep, 2023", "Early Access,Clicker,Strategy",
                          '£3.39', '£2.54', True, False, False, "Adventure,Early Access",
                          "Fake, Fake2", "Fake"]],
                        columns=['app_id', 'title', 'release_date',
                        'user_tags', 'full price', 'sale price',
                                 'windows', 'mac', 'linux', 'genres', 'developers', 'publishers'])


@pytest.fixture
def fake_data_with_tags() -> pd.DataFrame:
    """Fake dataframe data with tags """
    return pd.DataFrame([[2246030, "Fake: Escape", "5 Sep, 2023", "Early Access,Clicker,Strategy",
                         '£3.39', '£2.54', True, False, False, "Adventure,Early Access",
                          "Fake", "Fake", "Early Access"]],
                        columns=['app_id', 'title', 'release_date', 'user_tags',
                                 'full price', 'sale price', 'windows', 'mac',
                                 'linux', 'genres', 'developers', 'publishers', 'genre'])


@pytest.fixture
def fake_publisher_data() -> pd.DataFrame:
    """Fake publisher data columns"""
    publisher = pd.DataFrame(
        [['fake publisher 1'], ['fake_publisher 2']], columns=['publishers'])
    return publisher['publishers']


@pytest.fixture
def fake_genre_data() -> pd.DataFrame:
    """Fake genre data columns"""
    genre = pd.DataFrame(
        [['fake_genre', True], ['fake 2', False]], columns=['genre', 'user_generated'])
    return genre[["genre", "user_generated", "genre", "user_generated"]]


@pytest.fixture
def fake_game_and_genre() -> pd.DataFrame:
    """Fake game and genre data for testing"""
    game_genre = pd.DataFrame(
        [[123, 'solo', True]],
        columns=['app_id', 'genre', 'user_generated']
    )
    return game_genre[['app_id', 'genre', 'user_generated']]


@pytest.fixture
def fake_game_and_publisher() -> pd.DataFrame:
    """Fake game and publisher data for testing"""
    game_genre = pd.DataFrame(
        [[123, 'publisher']],
        columns=['app_id', 'publishers']
    )
    return game_genre[['app_id', 'publishers']]


@pytest.fixture
def fake_game_and_developer() -> pd.DataFrame:
    """Fake game and developer data for testing"""
    game_genre = pd.DataFrame(
        [[123, 'developer']],
        columns=['app_id', 'developers']
    )
    return game_genre[['app_id', 'developers']]


@pytest.fixture
def fake_game_data() -> pd.DataFrame:
    """Fake game dataframe columns"""
    genre = pd.DataFrame(
        [[1, 'fake_title 1', '2023-09-05', 5.30, 5.30, 1],
         [3, 'fake_title 2', '2023-09-05', 5.30, 4.30, 2]],
        columns=['app_id', 'title', 'release_date', 'price', 'sale_price', 'platform_id'])
    return genre[['app_id', 'title', 'release_date', 'price', 'sale_price', 'platform_id']]


@pytest.fixture
def fake_tuples() -> list[tuple]:
    """Fake tuples of id"""
    return [(1, 2), (3, 4)]


@pytest.fixture
def fake_complete_data() -> pd.DataFrame:
    """Fake final game dataframe"""
    return pd.DataFrame(
        [[1, 'fake_title 1', '2023-09-05', 5.30, 5.30, True, False, True,
          'fake developer', 'fake publisher', 'hiphop', True],
            [2, 'fake_title 2', '2023-09-05', 5.30, 4.30, True, False, True,
             'fake developer 1', 'fake publisher 2', 'rock', True]],
        columns=['app_id', 'title', 'release_date', 'full_price', 'sale_price',
                 'windows', 'mac', 'linux', 'developers', 'publishers', 'genre', 'user generated'])
