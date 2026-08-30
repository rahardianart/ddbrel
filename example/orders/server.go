package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"github.com/rahardianart/ddbrel"
)

type api struct{ svc *Service }

func (a *api) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /orders", a.placeOrder)
	mux.HandleFunc("GET /users/{id}/orders", a.recentOrders)
	mux.HandleFunc("DELETE /users/{id}/edges", a.closeAccount)
	mux.HandleFunc("GET /orders/{id}/buyer", a.buyer)
	mux.HandleFunc("POST /orders/{id}/tags", a.addTag)
	mux.HandleFunc("GET /orders/{id}/tags", a.tags)
	mux.HandleFunc("GET /tags/{id}/orders", a.ordersWithTag)
	mux.HandleFunc("GET /nodes/{id}/edges", a.edges)
	return logging(mux)
}

func (a *api) placeOrder(w http.ResponseWriter, r *http.Request) {
	var body struct {
		User  string `json:"user"`
		ID    string `json:"id"`
		Total int    `json:"total"`
		At    string `json:"at"`
	}
	if !decode(w, r, &body) {
		return
	}
	if body.User == "" || body.ID == "" || body.At == "" {
		writeErr(w, http.StatusBadRequest, errors.New("user, id and at are required"))
		return
	}

	err := a.svc.PlaceOrder(r.Context(), body.User, Order{ID: body.ID, Total: body.Total}, body.At)
	if err != nil {
		writeErr(w, statusFor(err), err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"order": body.ID, "user": body.User})
}

func (a *api) recentOrders(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeErr(w, http.StatusBadRequest, errors.New("limit must be a positive integer"))
			return
		}
		limit = n
	}

	orders, missing, err := a.svc.RecentOrders(r.Context(), r.PathValue("id"), limit)
	if err != nil {
		writeErr(w, statusFor(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"orders": orders, "missing": missing})
}

func (a *api) buyer(w http.ResponseWriter, r *http.Request) {
	id, err := a.svc.BuyerOf(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, statusFor(err), err)
		return
	}
	if id == "" {
		writeErr(w, http.StatusNotFound, errors.New("no buyer for that order"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"buyer": id})
}

func (a *api) addTag(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Tag string `json:"tag"`
		At  string `json:"at"`
	}
	if !decode(w, r, &body) {
		return
	}
	if body.Tag == "" || body.At == "" {
		writeErr(w, http.StatusBadRequest, errors.New("tag and at are required"))
		return
	}

	if err := a.svc.Tag(r.Context(), r.PathValue("id"), body.Tag, body.At); err != nil {
		writeErr(w, statusFor(err), err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"order": r.PathValue("id"), "tag": body.Tag})
}

func (a *api) tags(w http.ResponseWriter, r *http.Request) {
	ids, err := a.svc.TagsOf(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, statusFor(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tags": ids})
}

func (a *api) ordersWithTag(w http.ResponseWriter, r *http.Request) {
	ids, err := a.svc.OrdersWithTag(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, statusFor(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"orders": ids})
}

func (a *api) edges(w http.ResponseWriter, r *http.Request) {
	found, err := a.svc.EverythingAbout(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, statusFor(err), err)
		return
	}

	out := make([]map[string]string, 0, len(found))
	for _, e := range found {
		out = append(out, map[string]string{"from": e.From, "label": e.Label, "to": e.To, "sort": e.Sort})
	}
	writeJSON(w, http.StatusOK, map[string]any{"edges": out})
}

func (a *api) closeAccount(w http.ResponseWriter, r *http.Request) {
	n, err := a.svc.CloseAccount(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, statusFor(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"removed": n})
}

// statusFor maps a rejected id or label to 400; everything else is a server or
// DynamoDB failure the caller cannot fix by changing the request.
func statusFor(err error) int {
	if errors.Is(err, ddbrel.ErrInvalidID) {
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}
