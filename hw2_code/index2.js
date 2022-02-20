const mysql = require("mysql2/promise");
const express = require("express");
const bodyParser = require("body-parser");

let connection, connection2;
(async () => {
  try {
    connection = await mysql.createConnection({
      host: "localhost",
      user: "master",
      password: "masterpw",
      database: "mydb",
    });

    connection2 = await mysql.createConnection({
      host: "34.70.221.20",
      user: "master2",
      password: "masterpw",
      database: "mydb",
    });

    connection.connect();
    connection2.connect();
  } catch (e) {
    console.log(e);
  }
})();

const app = express();

app.use(bodyParser.json());

app.get("/greeting", async (req, res) => {
  return res.send("“Hello World!");
});

app.post("/register", async (req, res) => {
  let n = req.body.username;
  n = n.replace(/^[0-9\s]*|[+*\r\n]/g, "");
  const query = `INSERT INTO Users (username) VALUES ('${n}');`;
  const [r1, f1] = await connection.query(query);
  console.log(r1);

  const [r2, f2] = await connection2.query(query);
  console.log(r2);

  return res.json({ message: "register user succeed" });
});

app.get("/list", async (req, res) => {
  const query = "SELECT * FROM Users;";
  let nameArr = [];
  // try to get from db1
  try {
    const [c1rows, c1fields] = await connection.query(query);
    for (const obj of c1rows) {
      nameArr.push(obj.username);
    }
    console.log(nameArr);
    return res.json({ users: nameArr });
  } catch (e) {
    console.log(e);
  }

  const [c2rows, c2fields] = await connection2.query(query);
  for (const obj of c2rows) {
    nameArr.push(obj.username);
  }
  console.log(nameArr);

  return res.json({ users: nameArr });
});

app.post("/clear", async (req, res) => {
  const query = "DELETE FROM Users;";

  const [r1, f1] = await connection.query(query);
  console.log(r1);

  const [r2, f2] = await connection2.query(query);
  console.log(r2);

  return res.json({ users: "delete succeed" });
});

let http = require("http").Server(app);

const PORT = 80;

http.listen(PORT, function () {
  console.log("start listen");
});
