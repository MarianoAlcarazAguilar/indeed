from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from  selenium.webdriver.common.actions.wheel_input import ScrollOrigin
from selenium.webdriver.common.action_chains import ActionChains


class Controler:
    def __init__(self, headless=False, dont_load_images=True):
        self.driver = self.__start_driver(headless=headless, dont_load_images=dont_load_images)
        self.active_controler = True
        self.headless = headless
        self.active_url = False

    def __start_driver(self, headless:bool=False, dont_load_images:bool=True):
        chrome_options = Options()
        if headless:
            chrome_options.add_argument('--headless')
        if dont_load_images:
            chrome_options.add_argument('--blink-settings=imagesEnabled=false')
        driver = webdriver.Chrome(options=chrome_options)
        return driver
        
    def open_url(self, url:str, maximize_window=False):
        '''
        Use this function to open a url
        '''
        assert self.active_controler, "Your driver is not active"
        self.driver.get(url)
        self.active_url = True
        if maximize_window:
            assert not self.headless, "Can't maximize window since your driver is headless"
            self.driver.maximize_window()
        
    def scroll_down_instant(self):
        '''
        Use htis function to scroll down in the page automatically.
        This might cause bot detection.
        '''
        assert self.active_controler, "Your driver is not active"
        assert self.active_url, "Can't scroll down since there is no given url"
        self.driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")

    def scroll_down_with_keys(self):
        '''
        Use this function to emmulate scrolling down with keys
        '''
        assert self.active_controler, "Your driver is not active"
        assert self.active_url, "Can't scroll down since there is no given url"
        random_element = self.driver.find_element(by=By.TAG_NAME, value='body')
        random_element.send_keys(Keys.PAGE_DOWN)

    def scroll_down_with_wheel(self, scroll_distance:int=500):
        '''
        Use this function to emmulate scrolling down with the wheel.
        This is the most natural way of scrolling down.
        '''
        assert self.active_controler, "Your driver is not active"
        assert self.active_url, "Can't scroll down since there is no given url"
        actions = ActionChains(self.driver)
        actions.scroll_by_amount(0,scroll_distance)
        actions.perform()

    def __click_by(self, by, identifier):
        '''
        Internal function to click stuff
        '''
        assert self.active_controler, "Your driver is not active"
        assert self.active_url, "Can't click object since there is no given url"
        WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((by, identifier))).click()

    def click_by_xpath(self, xpath:str):
        '''
        This function clicks an object given an xpath
        '''
        self.__click_by(by=By.XPATH, identifier=xpath)
        

    def click_by_link_text(self, link_text:str):
        '''
        Use this functon to click an element given a link text
        '''
        self.__click_by(by=By.LINK_TEXT, identifier=link_text)

    def get_html(self, location:str=None):
        '''
        This function gets the html of a given page.
        If location is given as an input, it stores the html in the specified location and returns None
        '''
        assert self.active_controler, "Your driver is not active"
        assert self.active_url, "Can't get html file since there is no given url"
        html = self.driver.page_source
        if location is None:
            return html
        else:
            with open(location, 'w') as f:
                f.write(html)

    def quit_driver(self):
        '''
        This functions quits the driver and sets the corresponding flags to False
        '''
        assert self.active_controler, "Your driver is not active"
        self.driver.quit()
        self.active_url = False
        self.active_controler = False

    def restart_driver(self):
        '''
        This functions opnes a new driver in case it was closed.
        Remember that the driver won't have an assigned url anymore
        '''
        assert not self.active_controler, "Your driver is already active"
        self.driver = self.__start_driver(self.headless)

    def scroll_right_on_element(self, by, value:str, distance:int=100):
        assert self.active_controler, "Your driver is not active"
        assert self.active_url, "Can't scroll down since there is no given url"

        element_to_move = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((by, value)))
        scroll_origin = ScrollOrigin(element_to_move, x_offset=0, y_offset=0)
        actions = ActionChains(self.driver)
        actions.move_to_element(element_to_move)
        actions.scroll_from_origin(scroll_origin, distance, 0)
        actions.perform()

    def get_driver(self):
        return self.driver
    
    def send_keys_by(self, by:By, value:str, keys, enter=False):
        '''
        Utiliza esta función para mandar teclas a un elemento en específico.
        Si enter=True presiona Enter en el elemento dado. Es útil para cuadros de búsqueda.
        '''
        assert self.active_controler, "Your driver is not active"
        assert self.active_url, "Can't scroll down since there is no given url"

        element = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((by, value)))   
        element.send_keys(keys) 

        if enter:
            element.send_keys(Keys.RETURN)

    def click_element_by(self, by:By, value:str):
        assert self.active_controler, "Your driver is not active"
        assert self.active_url, "Can't scroll down since there is no given url"

        element = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((by, value)))  
        element.click()

    def element_exists(self, by:By, value:str, wait=False, timeout=2) -> bool:
        '''
        Verifica la existencia de un elemento. Regresa True o False
        '''
        assert self.active_controler, "Your driver is not active"
        assert self.active_url, "Can't scroll down since there is no given url"

        try:
            if wait:
                WebDriverWait(self.driver, timeout).until(EC.presence_of_element_located((by, value)))  
            else:
                self.driver.find_element(by, value)
            return True
        except:
            return False


        

if __name__ == "__main__":
    print('selenium_controler.py is running as main.')
    