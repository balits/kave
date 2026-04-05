{{/*  kave/templates/_helpers.tpl  */}}

{{- define "kave.name" -}}
{{ .Chart.Name }}
{{- end }}

{{- define "kave.headlessServiceName" -}}
{{ .Chart.Name }}-headless
{{- end }}

{{- define "kave.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{- define "kave.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
app.kubernetes.io/name: {{ include "kave.name" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}