package kz.kassayev.hibrain.service;

import io.vertx.core.json.JsonObject;
import rx.Completable;
import rx.Observable;
import rx.Single;

public interface Crud {
  Single<JsonObject> createEmployee(JsonObject item);

  Observable<JsonObject> getAllEmployees();

  Single<JsonObject> getEmployeeById(long id);

  Completable updateEmployee(long id, JsonObject item);

  Completable deleteEmployee(long id);
}
