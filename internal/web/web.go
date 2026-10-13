// Package web owns the HTML/CSS/JS the server renders. Templates are
// embedded with //go:embed so the shipped binary carries everything.
package web

import (
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"strings"
	"time"
)

//go:embed templates static
var files embed.FS

var funcMap = template.FuncMap{
	"shortID": func(s string) string {
		if len(s) <= 8 {
			return s
		}
		return s[:8]
	},
	"pctOf": func(n, total int64) string {
		if total == 0 {
			return "0.0%"
		}
		return formatFloat(float64(n)*100/float64(total), 1) + "%"
	},
	"upper":   strings.ToUpper,
	"now":     time.Now,
	"ms":      func(d time.Duration) int64 { return d.Milliseconds() },
}

// Templates holds the parsed template tree. Each page is parsed with
// the shared layout so {{ template "layout" . }} works everywhere.
type Templates struct {
	pages map[string]*template.Template
}

func Parse() (*Templates, error) {
	t := &Templates{pages: map[string]*template.Template{}}

	pageNames := []string{"dashboard", "jobs", "dlq", "config", "job_detail"}
	for _, name := range pageNames {
		tmpl, err := template.New(name).Funcs(funcMap).ParseFS(
			files,
			"templates/layout.html",
			"templates/partials/*.html",
			"templates/"+name+".html",
		)
		if err != nil {
			return nil, err
		}
		t.pages[name] = tmpl
	}

	// Standalone partials rendered as HTMX responses.
	partials := []string{"tiles", "jobs_rows", "dlq_rows", "activity_rows", "toast"}
	for _, name := range partials {
		tmpl, err := template.New(name).Funcs(funcMap).ParseFS(
			files,
			"templates/partials/"+name+".html",
		)
		if err != nil {
			return nil, err
		}
		t.pages["partial:"+name] = tmpl
	}

	return t, nil
}

func (t *Templates) Render(w http.ResponseWriter, name string, data any) error {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	tmpl, ok := t.pages[name]
	if !ok {
		http.Error(w, "template not found: "+name, http.StatusInternalServerError)
		return nil
	}
	entry := "layout"
	if strings.HasPrefix(name, "partial:") {
		entry = strings.TrimPrefix(name, "partial:")
	}
	return tmpl.ExecuteTemplate(w, entry, data)
}

// StaticFS exposes the embedded static/ directory as an http.FileSystem
// so Gin's StaticFS can serve it under /static.
func StaticFS() http.FileSystem {
	sub, err := fs.Sub(files, "static")
	if err != nil {
		panic(err)
	}
	return http.FS(sub)
}

func formatFloat(f float64, dp int) string {
	return fmt.Sprintf("%.*f", dp, f)
}
