class KaveClient {
    private baseUrl: string
    private otClient: OTClient
    constructor(baseUrl: string, otClient: OTClient) {
        this.baseUrl = baseUrl
        this.otClient = otClient
    }
}

class OTClient {
}