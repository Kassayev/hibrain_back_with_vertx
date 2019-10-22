package kz.kassayev.hibrain.service.impl;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.rxjava.ext.jdbc.JDBCClient;
import io.vertx.rxjava.ext.sql.SQLRowStream;
import kz.kassayev.hibrain.service.Crud;
import rx.Completable;
import rx.Observable;
import rx.Single;

import java.util.NoSuchElementException;
import java.util.Optional;

public class CrudImpl implements Crud {

  private static final String INSERT = "INSERT INTO employees (firstName, lastName, patronymic, telephone, email, country, city) VALUES(?,?,?,?,?,?,?)";

  private static final String SELEECT_ALL = "SELECT * FROM employees";

  private static final String UPDATE = "UPDATE employees SET firstName = ?, lastName = ?, patronymic = ?, telephone = ?, email = ?, country = ?, city = ? WHERE id = ?";

  private static final String DELETE = "DELETE FROM employees FROM id = ?";

  private static final String SELECT_ONE = "SELECT * FROM employees WHERE id = ?";

  private final JDBCClient db;

  public CrudImpl(JDBCClient db) {
    this.db = db;
  }

  @Override
  public Single<JsonObject> createEmployee(JsonObject item) {
    Optional<Exception> error = validateRequestBody(item);
    if (validateRequestBody(item).isPresent()) {
      return Single.error(error.get());
    }
    return db.rxGetConnection()
      .map(con -> con.setOptions(new SQLOptions().setAutoGeneratedKeys(true)))
      .flatMap(conn -> {
        JsonArray params = new JsonArray()
          .add(item.getValue("firstName"))
          .add(item.getValue("lastName"))
          .add(item.getValue("patronymic"))
          .add(item.getValue("telephone"))
          .add(item.getValue("email"))
          .add(item.getValue("country"))
          .add(item.getValue("city", 0));
        return conn
          .rxUpdateWithParams(INSERT, params)
          .map(ur -> item.put("id", ur.getKeys().getLong(0)))
          .doAfterTerminate(conn::close);
      });
  }

  @Override
  public Observable<JsonObject> getAllEmployees() {
    return db.rxGetConnection()
      .flatMapObservable(conn ->
        conn
          .rxQueryStream(SELEECT_ALL)
          .flatMapObservable(SQLRowStream::toObservable)
          .doAfterTerminate(conn::close))
      .map(array ->
        new JsonObject()
          .put("firstName", array.getString(1))
          .put("lastName", array.getString(2))
          .put("patronymic", array.getString(3))
          .put("telephone", array.getString(4))
          .put("email", array.getString(5))
          .put("country", array.getString(6))
          .put("city", array.getString(7))
      );
  }

  @Override
  public Single<JsonObject> getEmployeeById(long id) {
    return db.rxGetConnection()
      .flatMap(conn -> {
        JsonArray param = new JsonArray().add(id);
        return conn
          .rxQueryWithParams(SELECT_ONE, param)
          .map(ResultSet::getRows)
          .flatMap(list -> {
            if (list.isEmpty()) {
              return Single.error(new NoSuchElementException("Item : " + id + "; not found"));
            } else {
              return Single.just(list.get(0));
            }
          })
          .doAfterTerminate(conn::close);
      });
  }

  @Override
  public Completable updateEmployee(long id, JsonObject item) {
    Optional<Exception> error = validateRequestBody(item);
    if (validateRequestBody(item).isPresent()) {
      return Completable.error(error.get());
    }
    return db.rxGetConnection()
      .flatMapCompletable(conn -> {
        JsonArray params = new JsonArray()
          .add(item.getValue("firstName"))
          .add(item.getValue("lastName"))
          .add(item.getValue("patronymic"))
          .add(item.getValue("telephone"))
          .add(item.getValue("email"))
          .add(item.getValue("country"))
          .add(item.getValue("city", 0))
          .add(id);
        return conn
          .rxUpdateWithParams(UPDATE, params)
          .flatMapCompletable(up -> {
            if (up.getUpdated() == 0) {
              return Completable.error(new NoSuchElementException("Unknown item : " + id + ";"));
            }
            return Completable.complete();
          })
          .doAfterTerminate(conn::close);
      });
  }

  @Override
  public Completable deleteEmployee(long id) {
    return db.rxGetConnection()
      .flatMapCompletable(conn -> {
        JsonArray params = new JsonArray().add(id);
        return conn.rxUpdateWithParams(DELETE, params)
          .flatMapCompletable(up -> {
            if (up.getUpdated() == 0) {
              return Completable.error(new NoSuchElementException("Unknown item : " + id + ";"));
            }
            return Completable.complete();
          })
          .doAfterTerminate(conn::close);
      });
  }

  private Optional<Exception> validateRequestBody(JsonObject item) {
    if (item == null) {
      return Optional.of(new IllegalArgumentException("The item most not be null"));
    }
    if (!(item.getValue("firstName") instanceof String) || item.getString("firstName") == null
      || item.getString("firstName").isEmpty()) {
      return Optional.of(new IllegalArgumentException("The firstName is required"));
    }
    if (!(item.getValue("lastName") instanceof String) || item.getString("lastName") == null
      || item.getString("lastName").isEmpty()) {
      return Optional.of(new IllegalArgumentException("The lastName is required"));
    }
    if (!(item.getValue("patronymic") instanceof String) || item.getString("patronymic") == null
      || item.getString("patronymic").isEmpty()) {
      return Optional.of(new IllegalArgumentException("The patronymic is required"));
    }
    if (!(item.getValue("telephone") instanceof String) || item.getString("telephone") == null
      || item.getString("telephone").isEmpty()) {
      return Optional.of(new IllegalArgumentException("The telephone is required"));
    }
    if (!(item.getValue("email") instanceof String) || item.getString("email") == null
      || item.getString("email").isEmpty()) {
      return Optional.of(new IllegalArgumentException("The email is required"));
    }
    if (!(item.getValue("country") instanceof String) || item.getString("country") == null
      || item.getString("country").isEmpty()) {
      return Optional.of(new IllegalArgumentException("The country is required"));
    }
    if (!(item.getValue("city") instanceof String) || item.getString("city") == null
      || item.getString("city").isEmpty()) {
      return Optional.of(new IllegalArgumentException("The city is required"));
    }
  /*  if (item.containsKey("id")) {
      return Optional.of(new IllegalArgumentException("ID was invalidly set on request"));
    }*/
    return Optional.empty();
  }
}