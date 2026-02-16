// Copyright 2024 Antonio Bono
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "nao_led_server/led_action_server.hpp"

#include <chrono>
#include <functional>

#include "rclcpp_components/register_node_macro.hpp"

namespace nao_led_action_server
{

using namespace std::chrono_literals;

static inline bool has_led(const std::array<uint8_t, 2> & leds, uint8_t idx)
{
  return leds[0] == idx || leds[1] == idx;
}

static inline bool is_color_zero_rgb(const std_msgs::msg::ColorRGBA & c)
{
  return (c.r == 0.0F && c.g == 0.0F && c.b == 0.0F);
}

static inline bool is_color_set_meaningful(const std_msgs::msg::ColorRGBA & c)
{
  // Consider "meaningful" if any RGB component is non-zero.
  // Alpha inconsistencies in some stacks can be annoying, so we normalize alpha later.
  return !is_color_zero_rgb(c);
}

static inline void normalize_alpha(std_msgs::msg::ColorRGBA & c)
{
  // Some stacks behave badly with alpha=0; force a sane default.
  if (c.a == 0.0F) {
    c.a = 1.0F;
  }
}

static inline std::array<std_msgs::msg::ColorRGBA, 8>
normalize_eye_colors(const std::array<std_msgs::msg::ColorRGBA, 8> & in)
{
  std::array<std_msgs::msg::ColorRGBA, 8> out = in;

  // Ensure alpha sane
  for (auto & c : out) {
    normalize_alpha(c);
  }

  // If profile provided only one color for eyes, replicate it to all 8. 
  // This is a common case since many effects only use one color, and it avoids 
  // having to repeat the same color 8 times in the profile.
  bool any_nonzero_after0 = false;
  for (int i = 1; i < 8; ++i) {
    if (is_color_set_meaningful(out[i])) {
      any_nonzero_after0 = true;
      break;
    }
  }

  if (!any_nonzero_after0 && is_color_set_meaningful(out[0])) {
    for (int i = 1; i < 8; ++i) {
      out[i] = out[0];
      normalize_alpha(out[i]);
    }
  }

  return out;
}

LedsPlayActionServer::LedsPlayActionServer(const rclcpp::NodeOptions & options)
: rclcpp::Node("leds_play_action_server_node", options)
{
  using std::placeholders::_1;
  using std::placeholders::_2;

  action_server_ = rclcpp_action::create_server<LedsPlay>(
    this,
    "leds_play",
    std::bind(&LedsPlayActionServer::handleGoal, this, _1, _2),
    std::bind(&LedsPlayActionServer::handleCancel, this, _1),
    std::bind(&LedsPlayActionServer::handleAccepted, this, _1));

  head_pub_ = create_publisher<nao_lola_command_msgs::msg::HeadLeds>("effectors/head_leds", 10);
  right_eye_pub_ =
    create_publisher<nao_lola_command_msgs::msg::RightEyeLeds>("effectors/right_eye_leds", 10);
  left_eye_pub_ =
    create_publisher<nao_lola_command_msgs::msg::LeftEyeLeds>("effectors/left_eye_leds", 10);
  right_ear_pub_ =
    create_publisher<nao_lola_command_msgs::msg::RightEarLeds>("effectors/right_ear_leds", 10);
  left_ear_pub_ =
    create_publisher<nao_lola_command_msgs::msg::LeftEarLeds>("effectors/left_ear_leds", 10);
  chest_pub_ = create_publisher<nao_lola_command_msgs::msg::ChestLed>("effectors/chest_led", 10);
  right_foot_pub_ =
    create_publisher<nao_lola_command_msgs::msg::RightFootLed>("effectors/right_foot_led", 10);
  left_foot_pub_ =
    create_publisher<nao_lola_command_msgs::msg::LeftFootLed>("effectors/left_foot_led", 10);

  color_off_.r = 0.0F;
  color_off_.g = 0.0F;
  color_off_.b = 0.0F;
  color_off_.a = 1.0F;

  RCLCPP_INFO(get_logger(), "LedsPlayActionServer Initialized");
}

LedsPlayActionServer::~LedsPlayActionServer()
{
  stop_requested_.store(true);

  std::lock_guard<std::mutex> lock(worker_mutex_);
  if (worker_.joinable()) {
    worker_.join();
  }
}

rclcpp_action::GoalResponse LedsPlayActionServer::handleGoal(
  const rclcpp_action::GoalUUID & uuid,
  std::shared_ptr<const nao_led_interfaces::action::LedsPlay::Goal> goal)
{
  RCLCPP_INFO(this->get_logger(), "Received LED goal request");
  (void)uuid;
  (void)goal;
  return rclcpp_action::GoalResponse::ACCEPT_AND_EXECUTE;
}

rclcpp_action::CancelResponse LedsPlayActionServer::handleCancel(
  const std::shared_ptr<GoalHandleLedsPlay> goal_handle)
{
  (void)goal_handle;
  RCLCPP_INFO(get_logger(), "Received request to cancel LED goal");
  return rclcpp_action::CancelResponse::ACCEPT;
}

void LedsPlayActionServer::handleAccepted(const std::shared_ptr<GoalHandleLedsPlay> goal_handle)
{
  std::shared_ptr<GoalHandleLedsPlay> previous_goal;
  {
    std::lock_guard<std::mutex> g(goal_mutex_);
    previous_goal = active_goal_.lock();
  }

  stop_requested_.store(true);
  {
    std::lock_guard<std::mutex> lock(worker_mutex_);
    if (worker_.joinable()) {
      worker_.join();
    }
  }

  if (previous_goal && previous_goal->is_active()) {
    auto res = std::make_shared<LedsPlay::Result>();
    res->success = false;
    previous_goal->abort(res);
    RCLCPP_INFO(get_logger(), "Previous goal aborted due to preemption");
  }

  stop_requested_.store(false);
  {
    std::lock_guard<std::mutex> g(goal_mutex_);
    active_goal_ = goal_handle;
  }

  std::lock_guard<std::mutex> lock(worker_mutex_);
  worker_ = std::thread([this, goal_handle]() { execute(goal_handle); });
}

bool LedsPlayActionServer::shouldStop(const std::shared_ptr<GoalHandleLedsPlay> & goal_handle) const
{
  
  /// Check if the current execution should stop, either due to:
  ///- ROS shutdown
  ///- Internal stop request (e.g. preemption)
  ///- Goal cancelation

  if (!rclcpp::ok()) {
    return true;
  }
  if (stop_requested_.load()) {
    return true;
  }
  if (goal_handle && goal_handle->is_canceling()) {
    return true;
  }
  return false;
}

void LedsPlayActionServer::execute(
  const std::shared_ptr<rclcpp_action::ServerGoalHandle<nao_led_interfaces::action::LedsPlay>>
    goal_handle)
{
  RCLCPP_INFO(this->get_logger(), "Executing LED goal");

  auto goal = goal_handle->get_goal();

  std::array<uint8_t, 2> leds = goal->leds;
  uint8_t mode = goal->mode;
  float frequency = goal->frequency;
  std::array<std_msgs::msg::ColorRGBA, 8> colors = goal->colors;
  std::array<float, 12> intensities = goal->intensities;
  float duration = goal->duration;

  auto result = std::make_shared<nao_led_interfaces::action::LedsPlay::Result>();
  bool stopped_early = false;

  if (mode == nao_led_interfaces::msg::LedModes::STEADY) {
    stopped_early = steadyMode(goal_handle, leds, colors, intensities, duration);
  } else if (mode == nao_led_interfaces::msg::LedModes::BLINKING) {
    stopped_early = blinkingMode(goal_handle, leds, frequency, colors, duration);
  } else if (mode == nao_led_interfaces::msg::LedModes::LOOP) {
    stopped_early = loopMode(goal_handle, leds, frequency, colors, duration);
  }

  if (stopped_early) {
    if (goal_handle->is_canceling()) {
      result->success = true;
      goal_handle->canceled(result);
      RCLCPP_INFO(this->get_logger(), "LED Goal canceled");
      return;
    }

    if (goal_handle->is_active()) {
      result->success = false;
      goal_handle->abort(result);
      RCLCPP_INFO(this->get_logger(), "LED Goal aborted (preempt/stop)");
    } else {
      RCLCPP_INFO(this->get_logger(), "LED Goal stopped; goal already not active");
    }
    return;
  }

  if (rclcpp::ok()) {
    result->success = true;
    goal_handle->succeed(result);
    RCLCPP_INFO(this->get_logger(), "LED Goal succeeded");
  }
}

void LedsPlayActionServer::publishOff(const std::array<uint8_t, 2> & leds)
{
  if (has_led(leds, LedIndexes::HEAD)) {
    nao_lola_command_msgs::msg::HeadLeds msg;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS; ++i) {
      msg.intensities[i] = 0.0F;
    }
    head_pub_->publish(msg);
  }

  if (has_led(leds, LedIndexes::REYE)) {
    nao_lola_command_msgs::msg::RightEyeLeds r;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEyeLeds::NUM_LEDS; ++i) {
      r.colors[i] = color_off_;
    }
    right_eye_pub_->publish(r);
  }

  if (has_led(leds, LedIndexes::LEYE)) {
    nao_lola_command_msgs::msg::LeftEyeLeds l;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEyeLeds::NUM_LEDS; ++i) {
      l.colors[i] = color_off_;
    }
    left_eye_pub_->publish(l);
  }

  if (has_led(leds, LedIndexes::REAR)) {
    nao_lola_command_msgs::msg::RightEarLeds r;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS; ++i) {
      r.intensities[i] = 0.0F;
    }
    right_ear_pub_->publish(r);
  }

  if (has_led(leds, LedIndexes::LEAR)) {
    nao_lola_command_msgs::msg::LeftEarLeds l;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEarLeds::NUM_LEDS; ++i) {
      l.intensities[i] = 0.0F;
    }
    left_ear_pub_->publish(l);
  }

  if (has_led(leds, LedIndexes::CHEST)) {
    nao_lola_command_msgs::msg::ChestLed msg;
    msg.color = color_off_;
    chest_pub_->publish(msg);
  }
}

bool LedsPlayActionServer::steadyMode(
  const std::shared_ptr<GoalHandleLedsPlay> & goal_handle,
  const std::array<uint8_t, 2> & leds,
  const std::array<std_msgs::msg::ColorRGBA, 8> & colors_in,
  const std::array<float, 12> & intensities,
  float duration_s)
{
  // Normalize eye color arrays so profiles with 1 color still drive all 8 LEDs.
  const auto eye_colors = normalize_eye_colors(colors_in);

  if (has_led(leds, LedIndexes::HEAD)) {
    nao_lola_command_msgs::msg::HeadLeds msg;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS; ++i) {
      msg.intensities[i] = intensities[i];
    }
    head_pub_->publish(msg);
  }

  if (has_led(leds, LedIndexes::REYE)) {
    nao_lola_command_msgs::msg::RightEyeLeds r;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEyeLeds::NUM_LEDS; ++i) {
      r.colors[i] = eye_colors[i];
    }
    right_eye_pub_->publish(r);
  }

  if (has_led(leds, LedIndexes::LEYE)) {
    nao_lola_command_msgs::msg::LeftEyeLeds l;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEyeLeds::NUM_LEDS; ++i) {
      l.colors[i] = eye_colors[i];
    }
    left_eye_pub_->publish(l);
  }

  if (has_led(leds, LedIndexes::REAR)) {
    nao_lola_command_msgs::msg::RightEarLeds r;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS; ++i) {
      r.intensities[i] = intensities[i];
    }
    right_ear_pub_->publish(r);
  }

  if (has_led(leds, LedIndexes::LEAR)) {
    nao_lola_command_msgs::msg::LeftEarLeds l;
    for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEarLeds::NUM_LEDS; ++i) {
      l.intensities[i] = intensities[i];
    }
    left_ear_pub_->publish(l);
  }

  if (has_led(leds, LedIndexes::CHEST)) {
    nao_lola_command_msgs::msg::ChestLed msg;
    msg.color = eye_colors[0];
    chest_pub_->publish(msg);
  }

  if (duration_s < 0.0F) {
    rclcpp::Rate rate(50.0);
    while (!shouldStop(goal_handle)) {
      rate.sleep();
    }
    publishOff(leds);
    return true;
  }

  if (duration_s == 0.0F) {
    return false;
  }

  const rclcpp::Time start = now();
  rclcpp::Rate rate(50.0);
  while (!shouldStop(goal_handle)) {
    if ((now() - start).seconds() >= duration_s) {
      break;
    }
    rate.sleep();
  }

  if (shouldStop(goal_handle)) {
    publishOff(leds);
    return true;
  }

  publishOff(leds);
  return false;
}

bool LedsPlayActionServer::blinkingMode(
  const std::shared_ptr<GoalHandleLedsPlay> & goal_handle,
  const std::array<uint8_t, 2> & leds,
  float frequency_hz,
  const std::array<std_msgs::msg::ColorRGBA, 8> & colors_in,
  float duration_s)
{
  if (frequency_hz <= 0.0F) {
    return true;
  }

  const auto eye_colors = normalize_eye_colors(colors_in);

  const rclcpp::Time start = now();
  rclcpp::Rate rate(frequency_hz);
  bool on = false;

  while (!shouldStop(goal_handle)) {
    if (duration_s >= 0.0F && (now() - start).seconds() >= duration_s) {
      break;
    }

    on = !on;

    if (has_led(leds, LedIndexes::HEAD)) {
      nao_lola_command_msgs::msg::HeadLeds msg;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS; ++i) {
        msg.intensities[i] = on ? 1.0F : 0.0F;
      }
      head_pub_->publish(msg);
    }

    if (has_led(leds, LedIndexes::REYE)) {
      nao_lola_command_msgs::msg::RightEyeLeds r;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEyeLeds::NUM_LEDS; ++i) {
        r.colors[i] = on ? eye_colors[i] : color_off_;
      }
      right_eye_pub_->publish(r);
    }

    if (has_led(leds, LedIndexes::LEYE)) {
      nao_lola_command_msgs::msg::LeftEyeLeds l;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEyeLeds::NUM_LEDS; ++i) {
        l.colors[i] = on ? eye_colors[i] : color_off_;
      }
      left_eye_pub_->publish(l);
    }

    if (has_led(leds, LedIndexes::REAR)) {
      nao_lola_command_msgs::msg::RightEarLeds r;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS; ++i) {
        r.intensities[i] = on ? 1.0F : 0.0F;
      }
      right_ear_pub_->publish(r);
    }

    if (has_led(leds, LedIndexes::LEAR)) {
      nao_lola_command_msgs::msg::LeftEarLeds l;
      for (unsigned i = 0; i < nao_lola_command_msgs::msg::LeftEarLeds::NUM_LEDS; ++i) {
        l.intensities[i] = on ? 1.0F : 0.0F;
      }
      left_ear_pub_->publish(l);
    }

    if (has_led(leds, LedIndexes::CHEST)) {
      nao_lola_command_msgs::msg::ChestLed msg;
      msg.color = on ? eye_colors[0] : color_off_;
      chest_pub_->publish(msg);
    }

    rate.sleep();
  }

  publishOff(leds);
  return shouldStop(goal_handle);
}

bool LedsPlayActionServer::loopMode(
  const std::shared_ptr<GoalHandleLedsPlay> & goal_handle,
  const std::array<uint8_t, 2> & leds,
  float frequency_hz,
  const std::array<std_msgs::msg::ColorRGBA, 8> & colors_in,
  float duration_s)
{
  if (frequency_hz <= 0.0F) {
    return true;
  }

  const auto eye_colors = normalize_eye_colors(colors_in);

  const rclcpp::Time start = now();
  rclcpp::Rate loop_rate(frequency_hz);

  uint8_t head_c = 0;
  uint8_t ear_c  = 0;
  int eye_step   = 0;

  const int eye_n  = nao_lola_command_msgs::msg::RightEyeLeds::NUM_LEDS;
  const int ear_n  = nao_lola_command_msgs::msg::RightEarLeds::NUM_LEDS;
  const int head_n = nao_lola_command_msgs::msg::HeadLeds::NUM_LEDS;

  while (!shouldStop(goal_handle)) {
    if (duration_s >= 0.0F && (now() - start).seconds() >= duration_s) {
      break;
    }

    if (has_led(leds, LedIndexes::HEAD)) {
      nao_lola_command_msgs::msg::HeadLeds msg;
      for (int i = 0; i < head_n; ++i) msg.intensities[i] = 1.0F;
      head_c = static_cast<uint8_t>((head_c + 1U) % head_n);
      msg.intensities[head_c] = 0.0F;
      head_pub_->publish(msg);
    }

    const bool do_reye = has_led(leds, LedIndexes::REYE);
    const bool do_leye = has_led(leds, LedIndexes::LEYE);

    if (do_reye || do_leye) {
      eye_step = (eye_step + 1) % eye_n;

      const int cw = eye_step;
      const int cw_succ = (cw + 1) % eye_n;
      const int ccw = (cw != 0) ? (eye_n - cw) : 0;
      const int ccw_succ = (ccw != 0) ? (ccw - 1) : (eye_n - 1);

      if (do_reye) {
        nao_lola_command_msgs::msg::RightEyeLeds r;
        for (int i = 0; i < eye_n; ++i) r.colors[i] = eye_colors[i];
        r.colors[cw] = color_off_;
        r.colors[cw_succ] = color_off_;
        right_eye_pub_->publish(r);
      }

      if (do_leye) {
        nao_lola_command_msgs::msg::LeftEyeLeds l;
        for (int i = 0; i < eye_n; ++i) l.colors[i] = eye_colors[i];
        l.colors[ccw] = color_off_;
        l.colors[ccw_succ] = color_off_;
        left_eye_pub_->publish(l);
      }
    }

    const bool do_rear = has_led(leds, LedIndexes::REAR);
    const bool do_lear = has_led(leds, LedIndexes::LEAR);

    if (do_rear || do_lear) {
      nao_lola_command_msgs::msg::RightEarLeds r;
      nao_lola_command_msgs::msg::LeftEarLeds l;

      for (int i = 0; i < ear_n; ++i) {
        if (do_rear) r.intensities[i] = 1.0F;
        if (do_lear) l.intensities[i] = 1.0F;
      }

      ear_c = static_cast<uint8_t>((ear_c + 1U) % ear_n);

      if (do_rear) r.intensities[ear_c] = 0.0F;
      if (do_lear) l.intensities[ear_c] = 0.0F;

      if (do_rear) right_ear_pub_->publish(r);
      if (do_lear) left_ear_pub_->publish(l);
    }

    loop_rate.sleep();
  }

  publishOff(leds);
  return shouldStop(goal_handle);
}

}  // namespace nao_led_action_server

RCLCPP_COMPONENTS_REGISTER_NODE(nao_led_action_server::LedsPlayActionServer)
