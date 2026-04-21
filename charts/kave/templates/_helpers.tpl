{{/*  kave/templates/_helpers.tpl  */}}

{{- define "kave.name" -}}
{{ .Chart.Name }}
{{- end }}

{{- define "kave.image" -}}
{{- if not .Values.image.tag -}}
{{- fail "image.tag must be set explicitly with --set image.tag=<sha>|latest" -}}
{{- end -}}
{{ .Values.image.registry }}/{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end }}

{{- define "kave.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
app.kubernetes.io/name: {{ include "kave.name" . }}
{{- end }}