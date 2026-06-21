#!/usr/bin/env bash

# Color codes for pretty printing
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}=====================================================${NC}"
echo -e "${CYAN}      Distributed Message Broker Verification        ${NC}"
echo -e "${CYAN}=====================================================${NC}"

# Cleanup function to kill background processes on exit
cleanup() {
    echo -e "\n${YELLOW}[*] Cleaning up background processes and temporary directories...${NC}"
    # Kill all running broker, manager, and demo processes
    pkill -f "./bin/manager" || true
    pkill -f "./bin/broker" || true
    pkill -f "./bin/demo" || true
    sleep 1
}
trap cleanup EXIT

# 1. Clean previous data
echo -e "${YELLOW}[*] Resetting log data directories...${NC}"
rm -rf data/
mkdir -p data/manager data/broker logs

# 2. Build the project
echo -e "${YELLOW}[*] Building binaries...${NC}"
./.go/bin/go build -o bin/manager cmd/manager/main.go || exit 1
./.go/bin/go build -o bin/broker cmd/broker/main.go || exit 1
./.go/bin/go build -o bin/demo cmd/demo/main.go || exit 1

# 3. Start Queue Manager (МО)
echo -e "${YELLOW}[*] Starting Queue Manager (МО) on port 8080...${NC}"
./bin/manager -port 8080 -state data/manager/state.json > logs/manager.log 2>&1 &
QM_PID=$!
sleep 1

# 4. Start Service Broker (СБ)
echo -e "${YELLOW}[*] Starting Service Broker (СБ) on port 8081...${NC}"
./bin/broker -id broker-1 -port 8081 -qm http://localhost:8080 -data data/broker > logs/broker-1.log 2>&1 &
BROKER_PID=$!
sleep 2 # Allow broker to register and establish heartbeat

# =====================================================================
# Scenario 1: DATA SAFETY
# =====================================================================
echo -e "\n${CYAN}-----------------------------------------------------${NC}"
echo -e "${CYAN} Scenario 1: Data Safety (Append-only + Fsync + Recover) ${NC}"
echo -e "${CYAN}-----------------------------------------------------${NC}"
echo -e "${YELLOW}[*] Publishing 1000 messages to 'safety-topic'...${NC}"

# Publish 1000 messages
./bin/demo -mode publish -topic safety-topic -count 1000 -payload "safety-msg" > logs/publish_safety.log 2>&1

echo -e "${YELLOW}[*] Killing Broker process (kill -9) to simulate unexpected crash...${NC}"
kill -9 $BROKER_PID
sleep 2

echo -e "${YELLOW}[*] Restarting the Service Broker...${NC}"
./bin/broker -id broker-1 -port 8081 -qm http://localhost:8080 -data data/broker > logs/broker-1-restart.log 2>&1 &
BROKER_PID=$!
sleep 2 # Let the broker reload log files and register

echo -e "${YELLOW}[*] Subscriber reading 1000 messages from restarted broker...${NC}"
./bin/demo -mode subscribe -topic safety-topic -group safety-group -id sub-safety -count 1000 > logs/subscribe_safety.log 2>&1

# Verify safety messages count
CONSUMED_COUNT=$(grep -c "Received: offset=" logs/subscribe_safety.log || true)
if [ "$CONSUMED_COUNT" -eq 1000 ]; then
    echo -e "${GREEN}[SUCCESS] Data Safety: Recovered and read all 1000 messages successfully!${NC}"
else
    echo -e "${RED}[FAILURE] Data Safety: Expected 1000 messages, but read $CONSUMED_COUNT. Check logs/subscribe_safety.log${NC}"
fi

# =====================================================================
# Scenario 2: LOAD BALANCING
# =====================================================================
echo -e "\n${CYAN}-----------------------------------------------------${NC}"
echo -e "${CYAN} Scenario 2: Load Balancing (Consumer Group Balancing) ${NC}"
echo -e "${CYAN}-----------------------------------------------------${NC}"
echo -e "${YELLOW}[*] Starting 2 subscriber instances in consumer group 'lb-group'...${NC}"

# Start subscriber A and B in background
./bin/demo -mode subscribe -topic lb-topic -group lb-group -id sub-A -count 5 -delay 200 -limit 1 > logs/sub_A.log 2>&1 &
SUB_A_PID=$!
./bin/demo -mode subscribe -topic lb-topic -group lb-group -id sub-B -count 5 -delay 200 -limit 1 > logs/sub_B.log 2>&1 &
SUB_B_PID=$!

sleep 1

echo -e "${YELLOW}[*] Publishing 10 messages to 'lb-topic'...${NC}"
./bin/demo -mode publish -topic lb-topic -count 10 -payload "lb-msg" > logs/publish_lb.log 2>&1

# Wait for subscribers to finish
sleep 4

# Check how many messages each subscriber processed
COUNT_A=$(grep -c "Received: offset=" logs/sub_A.log || true)
COUNT_B=$(grep -c "Received: offset=" logs/sub_B.log || true)

echo -e "Subscriber A processed: ${CYAN}$COUNT_A${NC} messages"
echo -e "Subscriber B processed: ${CYAN}$COUNT_B${NC} messages"

if [ "$COUNT_A" -gt 0 ] && [ "$COUNT_B" -gt 0 ] && [ $((COUNT_A + COUNT_B)) -eq 10 ]; then
    echo -e "${GREEN}[SUCCESS] Load Balancing: Messages were distributed across subscribers. Total processed: 10/10.${NC}"
else
    echo -e "${RED}[FAILURE] Load Balancing: Distribution failed or messages were missed. Total processed: $((COUNT_A + COUNT_B))/10.${NC}"
fi

# =====================================================================
# Scenario 3: RESTART FROM OFFSET
# =====================================================================
echo -e "\n${CYAN}-----------------------------------------------------${NC}"
echo -e "${CYAN} Scenario 3: Restart from Offset (Resuming Processing) ${NC}"
echo -e "${CYAN}-----------------------------------------------------${NC}"
echo -e "${YELLOW}[*] Publishing 5 messages to 'restart-topic'...${NC}"
./bin/demo -mode publish -topic restart-topic -count 5 -payload "restart-msg-part1" > logs/publish_restart_1.log 2>&1

echo -e "${YELLOW}[*] Starting Subscriber A to read all 5 initial messages and stop...${NC}"
./bin/demo -mode subscribe -topic restart-topic -group restart-group -id sub-restart -count 5 > logs/sub_restart_1.log 2>&1

echo -e "${YELLOW}[*] Publishing 5 more messages to 'restart-topic' while Subscriber is offline...${NC}"
./bin/demo -mode publish -topic restart-topic -count 5 -payload "restart-msg-part2" > logs/publish_restart_2.log 2>&1

echo -e "${YELLOW}[*] Restarting Subscriber A to read remaining messages...${NC}"
./bin/demo -mode subscribe -topic restart-topic -group restart-group -id sub-restart -count 5 > logs/sub_restart_2.log 2>&1

# Verify that the second run consumed starting from offset 6 (message payload index 1-5 for part2)
FIRST_OFFSET=$(grep -o "offset=[0-9]*" logs/sub_restart_2.log | head -n 1 | cut -d= -f2 || true)
LAST_OFFSET=$(grep -o "offset=[0-9]*" logs/sub_restart_2.log | tail -n 1 | cut -d= -f2 || true)
RESUMED_COUNT=$(grep -c "Received: offset=" logs/sub_restart_2.log || true)

echo -e "Resumed subscriber read: ${CYAN}$RESUMED_COUNT${NC} messages"
echo -e "Offsets read in second run: ${CYAN}from $FIRST_OFFSET to $LAST_OFFSET${NC}"

if [ "$FIRST_OFFSET" -eq 6 ] && [ "$LAST_OFFSET" -eq 10 ] && [ "$RESUMED_COUNT" -eq 5 ]; then
    echo -e "${GREEN}[SUCCESS] Restart from Offset: Subscriber correctly resumed from offset 6.${NC}"
else
    echo -e "${RED}[FAILURE] Restart from Offset: Resume failed or incorrect offsets read: FirstOffset=$FIRST_OFFSET, LastOffset=$LAST_OFFSET.${NC}"
fi

# =====================================================================
# Scenario 4: MULTIPLE GROUPS
# =====================================================================
echo -e "\n${CYAN}-----------------------------------------------------${NC}"
echo -e "${CYAN} Scenario 4: Multiple Groups (Independent Broadcast)   ${NC}"
echo -e "${CYAN}-----------------------------------------------------${NC}"
echo -e "${YELLOW}[*] Publishing 5 messages to 'multigroup-topic'...${NC}"
./bin/demo -mode publish -topic multigroup-topic -count 5 -payload "multi-msg" > logs/publish_multigroup.log 2>&1

echo -e "${YELLOW}[*] Running Subscriber Group A...${NC}"
./bin/demo -mode subscribe -topic multigroup-topic -group group-A -id sub-groupA -count 5 > logs/sub_groupA.log 2>&1

echo -e "${YELLOW}[*] Running Subscriber Group B...${NC}"
./bin/demo -mode subscribe -topic multigroup-topic -group group-B -id sub-groupB -count 5 > logs/sub_groupB.log 2>&1

COUNT_A_MG=$(grep -c "Received: offset=" logs/sub_groupA.log || true)
COUNT_B_MG=$(grep -c "Received: offset=" logs/sub_groupB.log || true)

echo -e "Group A processed: ${CYAN}$COUNT_A_MG${NC} messages"
echo -e "Group B processed: ${CYAN}$COUNT_B_MG${NC} messages"

if [ "$COUNT_A_MG" -eq 5 ] && [ "$COUNT_B_MG" -eq 5 ]; then
    echo -e "${GREEN}[SUCCESS] Multiple Groups: Both groups consumed all 5 messages independently.${NC}"
else
    echo -e "${RED}[FAILURE] Multiple Groups: Failed to deliver messages independently. Group A count: $COUNT_A_MG, Group B count: $COUNT_B_MG.${NC}"
fi

# =====================================================================
# Scenario 5: MULTI-BROKER AND STATUS CHECK
# =====================================================================
echo -e "\n${CYAN}-----------------------------------------------------${NC}"
echo -e "${CYAN} Scenario 5: Multi-Broker Routing and Status API      ${NC}"
echo -e "${CYAN}-----------------------------------------------------${NC}"
echo -e "${YELLOW}[*] Starting a second broker instance 'broker-2' on port 8082...${NC}"
./bin/broker -id broker-2 -port 8082 -qm http://localhost:8080 -data data/broker > logs/broker-2.log 2>&1 &
BROKER_2_PID=$!
sleep 2

echo -e "${YELLOW}[*] Publishing to a new topic 'topic-broker2'...${NC}"
./bin/demo -mode publish -topic topic-broker2 -count 3 -payload "broker2-msg" > logs/publish_broker2.log 2>&1

echo -e "${YELLOW}[*] Querying /status endpoint on Queue Manager (МО):${NC}"
./bin/demo -mode status

echo -e "\n${CYAN}=====================================================${NC}"
echo -e "${CYAN}           Verification Process Finished             ${NC}"
echo -e "${CYAN}=====================================================${NC}"
