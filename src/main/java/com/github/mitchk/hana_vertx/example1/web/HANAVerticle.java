package com.github.mitchk.hana_vertx.example1.web;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class HANAVerticle extends AbstractVerticle {
	
	private JDBCClient client;


	@Override
	public void start(Future<Void> fut) {
		
// SIMPLE HTTP SERVER
//		vertx
//		  .createHttpServer()
//		  .requestHandler(req -> {
//			  req.response().headers().set("Content-Type", "text/plain");
//			  req.response().end("Hello World");
//		  })
//		  .websocketHandler(ws -> {
//			  ws.writeFinalTextFrame("Hello World");
//		  })
//		  .listen(8080);

		JsonObject config = new JsonObject();
		
		// Example connection string "jdbc:sap://hostname:30015/?autocommit=false"
		config.put("url", System.getenv("HANA_URL")); 
		config.put("driver_class", "com.sap.db.jdbc.Driver");
		config.put("user", System.getenv("HANA_USER"));
		config.put("password", System.getenv("HANA_PASSWORD"));

		client = JDBCClient.createShared(vertx, config); // , "java:comp/env/jdbc/DefaultDB");
		
		// Alternatively, you can use JNDI data sources, as well.
		// client = JDBCClient.createShared(vertx, config, "java:comp/env/jdbc/DefaultDB");

		Router router = Router.router(vertx);

		router.get("/api/helloWorld").handler(this::helloWorldHandler);

		vertx.createHttpServer()
			.requestHandler(router::accept)
			.listen(
				// Retrieve the port from the configuration,
				// default to 8080.
				config().getInteger("http.port", 8080), result -> {
					if (result.succeeded()) {
						fut.complete();
					} else {
						fut.fail(result.cause());
					}
				});
	}

	private void helloWorldHandler(RoutingContext routingContext) {
		
// HELLO WORLD WITHOUT HANA:
//		JsonObject obj = new JsonObject();
//		obj.put("message", "Hello World");
//
//		routingContext
//			.response().setStatusCode(200)
//			.putHeader("content-type", "application/json; charset=utf-8")
//			.end(Json.encodePrettily(obj));
		
		client.getConnection(res -> {
			if (!res.succeeded()) {
				System.err.println(res.cause());
				
				JsonObject obj = new JsonObject();
				obj.put("error", res.cause());
				routingContext.response().setStatusCode(500)
						.putHeader("content-type", "application/json; charset=utf-8").end(Json.encodePrettily(obj));
				return;
			}
			
			SQLConnection connection = res.result();
			
			connection.query("SELECT 'Hello World' AS GREETING FROM DUMMY", res2 -> {
				if (!res2.succeeded()) {
					System.err.println(res2.cause());
					
					
					JsonObject obj = new JsonObject();
					obj.put("error", res2.cause());

					routingContext.response().setStatusCode(500)
							.putHeader("content-type", "application/json; charset=utf-8")
							.end(Json.encodePrettily(obj));
					return;
				}

				ResultSet rs = res2.result();

				routingContext
					.response()
					.putHeader("content-type", "application/json; charset=utf-8")
					.end(Json.encodePrettily(rs));
			});
		});

	}
}
