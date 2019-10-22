package kz.kassayev.hibrain;

import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.http.HttpServer;
import io.vertx.rxjava.core.http.HttpServerResponse;
import io.vertx.rxjava.ext.jdbc.JDBCClient;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.ext.web.handler.BodyHandler;
import io.vertx.rxjava.ext.web.handler.CorsHandler;
import io.vertx.rxjava.ext.web.handler.StaticHandler;
import kz.kassayev.hibrain.initor.DBinit;
import rx.Single;
import kz.kassayev.hibrain.service.Crud;
import kz.kassayev.hibrain.service.impl.CrudImpl;

import java.util.NoSuchElementException;

import static kz.kassayev.hibrain.exception.Errors.error;

public class MainVerticle extends AbstractVerticle {

  private Crud crud;

  @Override
  public void start() throws Exception {
    Router router = Router.router(vertx);

    CorsHandler corsHandler = CorsHandler.create("*");
    corsHandler
      .allowedMethod(HttpMethod.GET)
      .allowedMethod(HttpMethod.DELETE)
      .allowedMethod(HttpMethod.POST)
      .allowedMethod(HttpMethod.PUT)
      .allowedMethod(HttpMethod.PATCH)
      .allowedHeader("Authorization")
      .allowedHeader("user-agent")
      .allowedHeader("Access-Control-Request-Method")
      .allowedHeader("Access-Control-Allow-Credentials")
      .allowedHeader("Access-Control-Allow-Origin")
      .allowedHeader("Access-Control-Allow-Headers")
      .allowedHeader("Content-Type")
//                        .allowCredentials(true);
      .allowedHeader("Content-Length")
      .allowedHeader("X-Requested-With")
      .allowedHeader("x-auth-token")
      .allowedHeader("Location")
      .exposedHeader("Location")
      .exposedHeader("Content-Type")
      .exposedHeader("Content-Length")
      .exposedHeader("ETag");

    router.route().handler(corsHandler);

    router.route().handler(BodyHandler.create());
    router.route("/api/employees/:id").handler(this::validateId);
    router.get("/api/employees").handler(this::retrieveAll);
    router.post("/api/employees").handler(this::addOne);
    router.get("/api/employees/:id").handler(this::getOne);
    router.put("/api/employee/:id").handler(this::updateOne);
    router.delete("/api/employee/:id").handler(this::deleteOne);

    router.get("/health").handler(rc -> rc.response().end("OK"));

    router.get().handler(StaticHandler.create());

    JDBCClient jdbc = JDBCClient.createShared(vertx, new JsonObject()
      .put("url", "jdbc:postgresql://" + getEnv("localhost", "localhost") + ":5432/postgres")
      .put("driver_class", "org.postgresql.Driver")
      .put("user", getEnv("DB_USERNAME", "postgres"))
      .put("password", getEnv("DB_PASSWORD", "kNOpKA228"))
    );

    DBinit.initDatabase(vertx, jdbc)
      .andThen(initHttpServer(router,jdbc))
      .subscribe(
        (http) -> System.out.println("Server ready on port : " + http.actualPort()),
        Throwable::printStackTrace
      );
  }

  private Single<HttpServer> initHttpServer(Router router, JDBCClient jdbcClient) {
    crud = new CrudImpl(jdbcClient);
    // Create the HTTP server and pass the "accept" method to the request handler.
    return vertx
      .createHttpServer()
      .requestHandler(router)
      .rxListen(8080);
  }

  private void validateId(RoutingContext ctx) {
    try {
      ctx.put("employeeId", Long.parseLong(ctx.pathParam("id")));
      ctx.next();
    } catch (NumberFormatException e) {
      error(ctx, 400, "invalid id: " + e.getCause());
    }
  }

  private void retrieveAll(RoutingContext ctx) {
    HttpServerResponse response = ctx.response()
      .putHeader("Content-Type", "application/json");
    JsonArray result = new JsonArray();
    crud.getAllEmployees()
      .subscribe(
        result::add,
        err -> error(ctx, 415, err),
        () -> response.end(result.encodePrettily())
      );
  }

  private void getOne(RoutingContext ctx) {
    HttpServerResponse response = ctx.response()
      .putHeader("Content-Type", "application/json");
    crud.getEmployeeById(ctx.get("employeeId"))
      .subscribe(
        json -> response.end(json.encodePrettily()),
        err -> {
          if (err instanceof NoSuchElementException) {
            error(ctx, 404, err);
          } else if (err instanceof IllegalArgumentException) {
            error(ctx, 415, err);
          } else {
            error(ctx, 500, err);
          }
        }
      );
  }

  private void getAboutError(RoutingContext ctx, Throwable err) {
    if (err instanceof NoSuchElementException) {
      error(ctx, 404, err);
    } else if (err instanceof IllegalArgumentException) {
      error(ctx, 422, err);
    } else {
      error(ctx, 409, err);
    }
  }

  private void addOne(RoutingContext ctx) {
    JsonObject item;
    try {
      item = ctx.getBodyAsJson();
    } catch (RuntimeException e) {
      error(ctx, 415, "invalid payload");
      return;
    }

    if (item == null) {
      error(ctx, 415, "invalid payload");
      return;
    }

    crud.createEmployee(item)
      .subscribe(
        json ->
          ctx.response()
            .putHeader("Location", "/api/employees/" + json.getLong("id"))
            .putHeader("Content-Type", "application/json")
            .setStatusCode(201)
            .end(json.encodePrettily()),
        err -> getAboutError(ctx, err)
      );
  }

  private void updateOne(RoutingContext ctx) {
    JsonObject item;
    try {
      item = ctx.getBodyAsJson();
    } catch (RuntimeException e) {
      error(ctx, 415, "invalid payload");
      return;
    }

    if (item == null) {
      error(ctx, 415, "invalid payload");
    }

    crud.updateEmployee(ctx.get("employeeId"), item)
      .subscribe(
        () ->
          ctx.response()
            .putHeader("Content-Type", "application/json")
            .setStatusCode(200)
            .end(item.put("id", ctx.<Long>get("employeeId")).encodePrettily()),
        err -> getAboutError(ctx, err)
      );
  }

  private void deleteOne(RoutingContext ctx) {
    crud.deleteEmployee(ctx.get("employeeId"))
      .subscribe(
        () ->
          ctx.response()
            .setStatusCode(204)
            .end(),
        err -> {
          if (err instanceof NoSuchElementException) {
            error(ctx, 404, err);
          } else {
            error(ctx, 415, err);
          }
        }
      );
  }

  private String getEnv(String key, String dv) {
    String str = System.getenv(key);
    if (str == null) {
      return dv;
    }
    return str;
  }
}
