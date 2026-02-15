import React from "react";
import ReactDOM from "react-dom/client";
import ProtestsPage from "./ProtestsPage";
import "./index.css";
import "./App.css";
import "./headlines.css";
import "./protests.css";

ReactDOM.createRoot(document.getElementById("protests-root")!).render(
  <React.StrictMode>
    <ProtestsPage />
  </React.StrictMode>,
);
