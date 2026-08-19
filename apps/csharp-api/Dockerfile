FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish Prism.Api.csproj -c Release -o /out --no-self-contained

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out .
ENV ASPNETCORE_URLS=http://0.0.0.0:8083
ENV HTTP_ADDR=0.0.0.0:8083
ENV PRISM_STORAGE=questdb
EXPOSE 8083
HEALTHCHECK --interval=10s --timeout=3s --retries=5 CMD wget -q -O- http://127.0.0.1:8083/healthz || exit 1
ENTRYPOINT ["dotnet", "Prism.Api.dll"]
