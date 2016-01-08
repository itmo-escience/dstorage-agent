package itmo.escience.dstorage.agent.handlers;

import itmo.escience.dstorage.agent.requests.AgentRequest;
import itmo.escience.dstorage.agent.responses.AgentResponse;

/**
 *
 * @author anton
 */
interface IRequestHandler {
    AgentResponse handle(AgentRequest request);
}
