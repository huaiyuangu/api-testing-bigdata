package BussinessLogics;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.hasItem;

import io.restassured.RestAssured;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestWithBeforeClass {

    public static void TestWithBeforeClass() {
        String uri = System.getProperty("server.host");
        if(uri==null){
            uri = "http://api.qrserver.com";
        }

        String path = System.getProperty("server.base");
        if(path==null){
            path = "/v1";
        }

        RestAssured.baseURI = uri;
        RestAssured.basePath = path;
    }

    public void CreateQRCodeWithBeforeClassData() {
        given().
            queryParam("data", "myDataForQRCode").  //mandatory
        when().
            post("/create-qr-code/").
        then().
            statusCode(200).
            log().everything();
    }

    public void CreateQRCodeOveridingBeforeClass() {
        RestAssured.baseURI = "https://api.punkapi.com";
        RestAssured.basePath = "/v2/beers?ids=1";

        given().
        when().
            get().
        then().
            body(
                "id", hasItem(1),
                "name", hasItem("Buzz"),
                "tagline", hasItem("A Real Bitter Experience.")
            ).
            log().everything();
    }
}
