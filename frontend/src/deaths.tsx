import React from "react";
import ReactDOM from "react-dom/client";
import DeathsPage from "./DeathsPage";
import "./index.css";
import "./App.css";
import "./headlines.css";
import "./deaths.css";

ReactDOM.createRoot(document.getElementById("deaths-root")!).render(
  <React.StrictMode>
    <DeathsPage />
  </React.StrictMode>,
);
