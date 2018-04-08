package grab

/**
  * Created by zongsizhang on 4/2/18.
  */
object JSHardCode {
  val html_head = "<!DOCTYPE html>\n<html>\n  <head>\n    <meta name=\"viewport\" content=\"initial-scale=1.0, user-scalable=no\">\n    <meta charset=\"utf-8\">\n    <title>Rectangles</title>\n    <style>\n      /* Always set the map height explicitly to define the size of the div\n       * element that contains the map. */\n      #map {\n        height: 100%;\n      }\n      /* Optional: Makes the sample page fill the window. */\n      html, body {\n        height: 100%;\n        margin: 0;\n        padding: 0;\n      }\n    </style>\n    <script>\n      function initMap() {\n        var map = new google.maps.Map(document.getElementById('map'), {\n          zoom: 11,\n          center: {lat: 40.7066965, lng: -73.978385},\n          mapTypeId: 'terrain'\n        });\n"
  val html_end = "}\n    </script>\n  </head>\n  <body>\n    <div id=\"map\"></div>\n    <script async defer\n    src=\"https://maps.googleapis.com/maps/api/js?key=AIzaSyBc8vhC4fzDgzIV5H3woa9wOBmEWmiVkkY&callback=initMap\">\n    </script>\n  </body>\n</html>"
  val block_head = "var rectangle = new google.maps.Rectangle({\n          strokeColor: '#FF0000',\n          strokeOpacity: 0.8,\n          strokeWeight: 1,\n          fillColor: '#FF0000',\n          map: map,\n"
}
